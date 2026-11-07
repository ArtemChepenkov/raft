# Этап 2: репликация журнала и REST API

## Что добавлено

- устойчивый журнал записей `{term, command}`;
- `POST /log/entries` для одной команды или пакета команд;
- `GET /log` для просмотра локального журнала любой ноды;
- репликация через `AppendEntries`;
- `next_index` и `match_index` лидера;
- проверка `prev_log_index/prev_log_term`;
- удаление конфликтующего неподтверждённого хвоста;
- продвижение `commit_index` после подтверждения большинством;
- догоняющая репликация после возвращения follower;
- проверка свежести журнала при голосовании.

## Быстрая проверка

Запустите три конфигурации в отдельных терминалах:

```powershell
python -m raft_stage1 --config raft_stage1/configs/node1.json
python -m raft_stage1 --config raft_stage1/configs/node2.json
python -m raft_stage1 --config raft_stage1/configs/node3.json
```

Найдите лидера через `/status`, затем добавьте запись именно на его порт:

```powershell
$body = @{
    command = @{ op = "set"; key = "x"; value = 10 }
    timeout_seconds = 5
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -ContentType application/json `
    -Uri http://127.0.0.1:8101/log/entries -Body $body
```

Пакет из нескольких команд:

```powershell
$body = @{
    commands = @(
        @{ op = "set"; key = "a"; value = 1 },
        @{ op = "set"; key = "b"; value = 2 }
    )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -ContentType application/json `
    -Uri http://127.0.0.1:8101/log/entries -Body $body
```

Просмотр локальных копий:

```powershell
Invoke-RestMethod http://127.0.0.1:8101/log
Invoke-RestMethod http://127.0.0.1:8102/log
Invoke-RestMethod http://127.0.0.1:8103/log
```

Пример ответа лидера после commit:

```json
{
  "term": 4,
  "first_index": 1,
  "last_index": 1,
  "commit_index": 1,
  "entries": [
    {
      "index": 1,
      "term": 4,
      "command": {"op": "set", "key": "x", "value": 10},
      "committed": true
    }
  ]
}
```

Если запрос отправлен follower, он отвечает `409`:

```json
{
  "error": "not_leader",
  "leader_id": "node1",
  "leader_url": "http://127.0.0.1:8101"
}
```

## Формат журнала

В памяти и на диске запись хранится без index:

```json
{"term": 4, "command": {"op": "set", "key": "x", "value": 10}}
```

Index определяется позицией в списке и начинается с единицы. REST-представление
добавляет вычисленные поля `index` и `committed`.

`state.json` теперь содержит:

```json
{
  "current_term": 4,
  "voted_for": "node1",
  "commit_index": 1,
  "log": [
    {"term": 4, "command": {"op": "set", "key": "x", "value": 10}}
  ]
}
```

Старый файл первого этапа без `log` и `commit_index` загружается как пустой
журнал. Запись выполняется через временный файл, `fsync` и атомарный `os.replace`.

## Добавление записи: цепочка функций

```text
POST /log/entries
-> RaftRequestHandler.do_POST()
-> _read_json_object()
-> _commands_from_payload()
-> RaftNode.append_commands()
-> дописать {term, command} в локальный журнал лидера
-> _save_stable_state_locked()
-> _broadcast_append_entries()
-> executor.submit(_replicate_to_peer) для каждого follower
```

HTTP-поток ждёт `Condition`, но не держит lock во время ожидания:

```text
append_commands()
-> Condition.wait()
-> worker получает подтверждение follower
-> _advance_commit_index_locked()
-> Condition.notify_all()
-> append_commands() просыпается
-> HTTP 201
```

Если кворум не появился до `timeout_seconds`, возвращается `504 commit_timeout`.
Запись при этом может остаться локальной и неподтверждённой; Raft не может
безопасно объявить её committed без большинства.

## Что отправляет лидер

Для каждого follower лидер хранит:

- `next_index` - индекс следующей записи, которую нужно попробовать отправить;
- `match_index` - максимальный индекс, который follower уже подтвердил.

Пример:

```text
leader log:              1 2 3 4
node2 next_index:        5      (уже всё получил)
node3 next_index:        3      (нужно отправить 3 и 4)
node3 match_index:       2
```

Цепочка одного RPC:

```text
_replicate_to_peer(node3)
-> next_index = 3
-> prev_log_index = 2
-> prev_log_term = term записи 2
-> entries = журнал[3..4]
-> POST /raft/append-entries
```

## Что делает follower

```text
handle_append_entries()
-> проверить term лидера
-> проверить наличие prev_log_index
-> проверить term записи prev_log_index
-> найти первый конфликт
-> удалить конфликтующий хвост
-> дописать записи лидера
-> commit_index = min(leader_commit, last_log_index)
-> сохранить состояние
-> вернуть success и match_index
```

Если предыдущая позиция не совпала, follower отвечает `success=false` и
`conflict_index`. Лидер уменьшает `next_index` и повторяет RPC с более раннего
места. Поэтому вернувшаяся после простоя нода автоматически догоняет лидера.

## Как запись становится committed

Лидер всегда считается первой копией. После каждого успешного ответа follower:

```text
match_index[peer] обновился
-> _advance_commit_index_locked()
-> найти максимальный index текущего term
-> посчитать ноды с match_index >= index
-> если count >= quorum: commit_index = index
```

Для трёх нод достаточно:

```text
leader + один follower = 2 копии = quorum
```

Ограничение `entry.term == current_term` соответствует правилу Raft: лидер
напрямую подтверждает большинством записи своего term. Вместе с такой записью
косвенно подтверждаются и все более ранние позиции журнала.

## Почему выборы теперь учитывают журнал

`RequestVote` передаёт настоящие `last_log_index` и `last_log_term`. Получатель
сначала сравнивает последний term, затем index. Кандидат с более старым журналом
не получает голос, поэтому подтверждённая запись не теряется при смене лидера.

## Heartbeat и репликация

Отдельного сообщения heartbeat нет:

```text
AppendEntries с entries=[]       -> heartbeat
AppendEntries с entries=[...]    -> репликация
```

Обе формы сбрасывают election timeout follower. После продвижения commit index
лидер планирует ближайший heartbeat, чтобы передать follower новое значение
`leader_commit`.

## Проверка тестами

```powershell
python -m unittest -v test_raft_stage1.py test_raft_stage2.py
```

Тесты второго этапа проверяют:

1. устойчивое хранение и замену конфликтующего хвоста;
2. отказ кандидату с менее свежим журналом;
3. REST-запись только через лидера;
4. commit после большинства;
5. догоняющую репликацию follower после остановки и повторного запуска.

## Ограничение этапа

`command` пока является произвольным JSON и только хранится в журнале. Она ещё не
применяется к key-value словарю. Это будет сделано на третьем этапе через
`last_applied` и последовательное применение committed-записей.
