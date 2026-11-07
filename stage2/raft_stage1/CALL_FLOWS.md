# Цепочки вызовов функций Raft stage 1

Этот файл показывает фактические цепочки вызовов текущей реализации. Обозначения:

```text
func1() -> func2()       func1 непосредственно вызывает func2
[RPC worker]             выполнение перешло в поток исходящих RPC
[HTTP node2]             выполнение продолжилось в HTTP-потоке другой ноды
[return]                 возврат из функции к вызывающему коду
```

Потоки работают параллельно, поэтому стрелки показывают причинную связь, а не
обязательно один и тот же стек Python.

## 1. Запуск ноды

```text
__main__
-> main()
-> build_parser()
-> ArgumentParser.parse_args()
-> load_config(path)
-> NodeConfig(...)
-> NodeConfig.validate()
-> RaftNode(config)
-> RaftNode.__init__()
-> NodeConfig.validate()
-> StableStateStore.__init__()
-> StableStateStore.load()
-> RaftNode._reset_election_deadline_locked()
-> RaftHTTPServer.__init__()
-> RaftNode.start()
```

После `RaftNode.start()` выполнение разветвляется:

```text
RaftNode.start()
-> ThreadPoolExecutor(...)
-> threading.Thread(target=_timer_loop)
-> Thread.start()
-> [timer thread] RaftNode._timer_loop()
```

Основной поток продолжает запуск HTTP-сервера:

```text
main()
-> RaftHTTPServer.serve_forever(poll_interval=0.1)
```

## 2. Запуск без сохранённого состояния

```text
RaftNode.__init__()
-> StableStateStore.load()
-> Path.exists() == False
-> [return] (current_term=0, voted_for=None)
-> role = FOLLOWER
-> _reset_election_deadline_locked()
```

## 3. Перезапуск с сохранённым состоянием

```text
RaftNode.__init__()
-> StableStateStore.load()
-> Path.read_text()
-> json.loads()
-> [return] (сохранённый current_term, сохранённый voted_for)
-> role = FOLLOWER
-> _reset_election_deadline_locked()
```

Роль лидера или кандидата не восстанавливается. После рестарта нода всегда
начинает как follower, но помнит term и уже отданный голос.

## 4. Обычная итерация таймера без действий

```text
_timer_loop()
-> _stopped.wait(0.02) == False
-> _clock()
-> lock.acquire()
-> прочитать role и deadlines
-> election_due = False
-> heartbeat_due = False
-> lock.release()
-> следующая итерация _timer_loop()
```

## 5. Follower не получил heartbeat и начинает выборы

```text
_timer_loop()
-> election_due = True
-> _start_election()
-> _clock()
-> role = CANDIDATE
-> current_term += 1
-> voted_for = self.node_id
-> votes_received = {self.node_id}
-> _reset_election_deadline_locked()
-> _save_stable_state_locked()
-> StableStateStore.save(current_term, voted_for)
```

Сохранение состояния:

```text
StableStateStore.save()
-> Path.mkdir()
-> temporary.open()
-> json.dump()
-> stream.flush()
-> os.fsync()
-> os.replace(temporary, state.json)
```

После сохранения кандидат запускает запросы голосов:

```text
_start_election()
-> ThreadPoolExecutor.submit(_request_vote_from_peer, node2, ...)
-> ThreadPoolExecutor.submit(_request_vote_from_peer, node3, ...)
```

## 6. Отправка RequestVote одному peer

```text
[RPC worker] _request_vote_from_peer(node2, url, election_term)
-> сформировать payload
-> _post_json(url + "/raft/request-vote", payload)
-> json.dumps(payload)
-> urllib.request.Request(...)
-> urlopen(request, timeout=rpc_timeout)
```

Поток находится внутри `urlopen()` до ответа, сетевой ошибки или истечения
`rpc_timeout`.

## 7. Node2 получает RequestVote и выдаёт голос

Сетевая граница:

```text
[node1] urlopen(POST /raft/request-vote)
-> [HTTP node2] RaftRequestHandler.do_POST()
```

Обработка на node2:

```text
do_POST()
-> _read_json_object()
-> headers.get_content_type()
-> rfile.read(content_length)
-> json.loads()
-> RaftNode.handle_request_vote(payload)
-> _required_non_negative_int(term)
-> _required_string(candidate_id)
-> _required_non_negative_int(last_log_index)
-> _required_non_negative_int(last_log_term)
-> проверить term, voted_for и актуальность журнала
-> _reset_election_deadline_locked()
-> _save_stable_state_locked()
-> StableStateStore.save()
-> [return] {term, vote_granted=True}
-> RaftRequestHandler._send_json(HTTPStatus.OK, response)
-> json.dumps()
-> send_response()/send_header()/end_headers()
-> wfile.write()
```

## 8. Получатель RequestVote увидел более высокий term

Например, node2 находится в term 4, а запрос пришёл с term 5:

```text
handle_request_vote(term=5)
-> term > current_term
-> _become_follower_locked(term=5, leader_id=None)
-> current_term = 5
-> voted_for = None
-> role = FOLLOWER
-> votes_received.clear()
-> _reset_election_deadline_locked()
-> _save_stable_state_locked()
-> StableStateStore.save()
-> продолжить проверку возможности голосования
```

Если остальные проверки успешны:

```text
handle_request_vote()
-> voted_for = candidate_id
-> _save_stable_state_locked()
-> StableStateStore.save()
-> [return] vote_granted=True
```

## 9. Получатель отклоняет RequestVote со старым term

```text
do_POST()
-> _read_json_object()
-> handle_request_vote(term=старый)
-> term < current_term
-> [return] {term=current_term, vote_granted=False}
-> _send_json(HTTPStatus.OK, response)
```

Состояние и election deadline в этой ветке не изменяются.

## 10. Получатель уже голосовал за другого кандидата

```text
handle_request_vote(candidate_id=node2)
-> may_vote = voted_for in (None, node2)
-> may_vote == False
-> vote_granted = False
-> [return] {term=current_term, vote_granted=False}
-> _send_json()
```

`StableStateStore.save()` повторно не вызывается, потому что голос не менялся.

## 11. Повторный RequestVote того же кандидата

Повтор возможен, если первый HTTP-ответ потерялся:

```text
handle_request_vote(candidate_id=node2)
-> voted_for == node2
-> may_vote == True
-> log_is_up_to_date == True
-> voted_for = node2
-> _reset_election_deadline_locked()
-> _save_stable_state_locked()
-> [return] vote_granted=True
```

Повторный одинаковый запрос безопасен: получатель подтверждает тот же голос, но не
голосует за другого кандидата.

## 12. Отказ из-за отстающего журнала

Подготовленная цепочка для второго этапа:

```text
handle_request_vote()
-> вычислить log_is_up_to_date
-> log_is_up_to_date == False
-> vote_granted = False
-> [return] {term=current_term, vote_granted=False}
```

Во втором этапе эта ветка достижима: локальные `last_log_term/last_log_index`
сравниваются с настоящим журналом кандидата.

## 13. Кандидат получает положительный ответ, но кворума ещё нет

Возврат ответа на node1:

```text
[HTTP node2] _send_json(vote_granted=True)
-> [node1 RPC worker] urlopen() возвращает response
-> response.read()
-> json.loads()
-> [return] из _post_json()
-> _request_vote_from_peer()
-> lock.acquire()
-> проверить role, current_term и election_term
-> votes_received.add(node2)
-> len(votes_received) < quorum
-> lock.release()
-> [return]
```

## 14. Кандидат получает голос, обеспечивающий кворум

```text
_request_vote_from_peer()
-> votes_received.add(peer_id)
-> len(votes_received) >= config.quorum
-> _become_leader_locked()
-> role = LEADER
-> leader_id = self.node_id
-> votes_received.clear()
-> next_heartbeat_at = _clock()
```

Полная межузловая цепочка успешных выборов:

```text
[node1 timer] _timer_loop()
-> _start_election()
-> StableStateStore.save()
-> ThreadPoolExecutor.submit()
-> [node1 RPC worker] _request_vote_from_peer(node2)
-> _post_json()
-> urlopen()
-> [HTTP node2] do_POST()
-> _read_json_object()
-> handle_request_vote()
-> StableStateStore.save()
-> _send_json(vote_granted=True)
-> [node1 RPC worker] _post_json() возвращает response
-> _request_vote_from_peer()
-> votes_received.add(node2)
-> _become_leader_locked()
```

## 15. Peer отказал кандидату в голосе

```text
_request_vote_from_peer()
-> _post_json()
-> [return] {term, vote_granted=False}
-> проверить response_term
-> не добавлять peer_id в votes_received
-> [return]
```

Кандидат остаётся кандидатом до heartbeat лидера, победы или следующего election
timeout.

## 16. Peer недоступен

```text
_request_vote_from_peer()
-> _post_json()
-> urlopen()
-> HTTPError/URLError/TimeoutError/OSError
-> lock.acquire()
-> rpc_errors += 1
-> lock.release()
-> [return] None
-> _request_vote_from_peer() пишет сообщение об отсутствии ответа
-> [return]
```

## 17. Пришёл некорректный ответ на RequestVote

```text
_request_vote_from_peer()
-> _post_json()
-> response_term отсутствует, bool или не int
-> LOGGER.warning()
-> [return]
```

Такой ответ не влияет на term и набор голосов.

## 18. Пришёл запоздалый положительный ответ старых выборов

```text
_request_vote_from_peer(election_term=4)
-> получить vote_granted=True
-> lock.acquire()
-> current_term != election_term
-> [return] без votes_received.add()
```

Аналогично ответ игнорируется, если нода уже не candidate или term ответа не
совпадает с term этих выборов.

## 19. Ответ RequestVote содержит более высокий term

```text
_request_vote_from_peer()
-> response_term > current_term
-> _become_follower_locked(response_term, leader_id=None)
-> current_term = response_term
-> voted_for = None
-> role = FOLLOWER
-> _reset_election_deadline_locked()
-> _save_stable_state_locked()
-> StableStateStore.save()
-> [return]
```

## 20. Голоса разделились, начинается новый term

```text
_start_election(term=4)
-> не получить quorum
-> candidate продолжает работу
-> _timer_loop()
-> election_deadline наступил
-> _start_election()
-> current_term = 5
-> voted_for = self.node_id
-> votes_received = {self.node_id}
-> StableStateStore.save()
-> новые RequestVote
```

## 21. Одноузловой кластер выбирает себя

Для конфигурации `peers={}` кворум равен единице:

```text
_timer_loop()
-> _start_election()
-> votes_received = {self.node_id}
-> len(votes_received) >= quorum
-> _become_leader_locked()
-> [return]
```

`ThreadPoolExecutor.submit()` и сетевые RPC в этом сценарии не вызываются.

## 22. Лидер планирует рассылку heartbeat

```text
_timer_loop()
-> heartbeat_due = True
-> _broadcast_append_entries()
-> проверить stopped и role == LEADER
-> term = current_term
-> next_heartbeat_at = _clock() + heartbeat_interval
-> heartbeats_sent += len(peers)
-> ThreadPoolExecutor.submit(_replicate_to_peer, node2, node2_url, term)
-> ThreadPoolExecutor.submit(_replicate_to_peer, node3, node3_url, term)
```

## 23. Отправка одного heartbeat

```text
[RPC worker] _replicate_to_peer(peer_id, peer_url, term)
-> сформировать AppendEntries из next_index follower
-> _post_json(peer_url + "/raft/append-entries", payload)
-> json.dumps()
-> Request(...)
-> urlopen(timeout=rpc_timeout)
```

## 24. Follower получает корректный heartbeat

```text
[leader RPC worker] urlopen(POST /raft/append-entries)
-> [HTTP follower] do_POST()
-> _read_json_object()
-> handle_append_entries()
-> _required_non_negative_int(term)
-> _required_string(leader_id)
-> проверить остальные поля
-> _become_follower_locked(term, leader_id)
-> role = FOLLOWER
-> leader_id = лидер
-> votes_received.clear()
-> _reset_election_deadline_locked()
-> last_leader_contact_at = _clock()
-> heartbeats_received += 1
-> [return] {term, success=True, match_index=0}
-> _send_json()
```

Если term не изменился, стабильное состояние повторно не записывается.

## 25. Candidate получает heartbeat победившего лидера

```text
[HTTP candidate] do_POST()
-> handle_append_entries()
-> _become_follower_locked(term, leader_id)
-> role: CANDIDATE -> FOLLOWER
-> votes_received.clear()
-> _reset_election_deadline_locked()
-> [return] success=True
```

## 26. Нода получает heartbeat с более высоким term

```text
handle_append_entries(term=новый)
-> _become_follower_locked(term, leader_id)
-> current_term = новый term
-> voted_for = None
-> role = FOLLOWER
-> _save_stable_state_locked()
-> StableStateStore.save()
-> [return] success=True
```

## 27. Нода отклоняет heartbeat со старым term

```text
do_POST()
-> handle_append_entries(term=старый)
-> term < current_term
-> [return] {term=current_term, success=False, match_index=0}
-> _send_json()
```

Роль, лидер и election deadline в этой ветке не меняются.

## 28. AppendEntries содержит записи или неправильную предыдущую позицию

Во втором этапе follower проверяет предыдущую позицию и принимает записи:

```text
handle_append_entries()
-> _become_follower_locked(term, leader_id)
-> проверить prev_log_index/prev_log_term
-> при конфликте удалить неподтверждённый хвост
-> дописать entries
-> обновить commit_index
-> [return] success=True и match_index
```

При несовпадении предыдущей позиции возвращаются `success=False` и
`conflict_index`, после чего лидер повторяет запрос с более раннего места.

## 29. Лидер получает обычный успешный ответ heartbeat

```text
_replicate_to_peer()
-> _post_json()
-> получить {term=текущий, success=True}
-> обновить match_index и next_index follower
-> _advance_commit_index_locked()
```

Если большинство подтвердило запись текущего term, лидер продвигает commit index.

## 30. Лидер получает ответ heartbeat с более высоким term

```text
_replicate_to_peer()
-> _post_json()
-> response_term > current_term
-> _become_follower_locked(response_term, leader_id=None)
-> role: LEADER -> FOLLOWER
-> current_term = response_term
-> voted_for = None
-> _reset_election_deadline_locked()
-> StableStateStore.save()
```

## 31. Heartbeat не получил ответа

```text
_replicate_to_peer()
-> _post_json()
-> urlopen()
-> сетевая ошибка или timeout
-> rpc_errors += 1
-> [return] None
-> _replicate_to_peer() завершает работу
```

Лидер не отказывается от роли из-за одного потерянного heartbeat.

## 32. GET /status

```text
клиент GET /status
-> RaftRequestHandler.do_GET()
-> urlsplit(self.path)
-> RaftNode.status()
-> lock.acquire()
-> _clock()
-> собрать снимок состояния
-> lock.release()
-> [return] dict
-> RaftRequestHandler._send_json(HTTPStatus.OK, dict)
-> json.dumps()
-> wfile.write()
```

`GET /` проходит по той же цепочке.

## 33. GET /health

```text
клиент GET /health
-> RaftRequestHandler.do_GET()
-> urlsplit(self.path)
-> RaftRequestHandler._send_json(HTTPStatus.OK, {status, node_id})
-> wfile.write()
```

`RaftNode.status()` здесь не вызывается.

## 34. Неизвестный GET или POST маршрут

GET:

```text
do_GET()
-> urlsplit()
-> _send_json(HTTPStatus.NOT_FOUND, {error})
```

POST сначала читает тело, затем проверяет маршрут:

```text
do_POST()
-> _read_json_object()
-> urlsplit()
-> _send_json(HTTPStatus.NOT_FOUND, {error})
```

## 35. Некорректный JSON или поля RPC

```text
do_POST()
-> _read_json_object() или handle_request_vote()/handle_append_entries()
-> raise ValueError
-> except ValueError
-> _send_json(HTTPStatus.BAD_REQUEST, {error})
```

Примеры: неправильный `Content-Type`, отсутствующий `Content-Length`, тело больше
1 MiB, сломанный JSON, отрицательный term или отсутствующий `candidate_id`.

## 36. Остановка через Ctrl+C или SIGTERM

```text
операционная система отправляет сигнал
-> request_stop()
-> stopped.set()
-> threading.Thread(target=server.shutdown).start()
-> [shutdown thread] RaftHTTPServer.shutdown()
-> [main thread] serve_forever() возвращает управление
-> finally
-> server.server_close()
-> node.stop()
-> _stopped.set()
-> timer_thread.join(timeout=2.0)
-> executor.shutdown(wait=True, cancel_futures=True)
-> main() возвращает 0
```

`server.shutdown()` вызывается в отдельном потоке, чтобы не ждать завершения
`serve_forever()` из того же потока, где этот цикл выполняется.

## 37. Остановка timer-потока

```text
node.stop()
-> _stopped.set()
-> [timer thread] _stopped.wait(0.02) возвращает True
-> not True == False
-> while завершается
-> _timer_loop() возвращает управление
-> [main thread] timer_thread.join() завершается
```

## 38. Полный сценарий: выборы, heartbeat, отказ лидера, новые выборы

```text
[node1 timer] _timer_loop()
-> _start_election(term=1)
-> _request_vote_from_peer(node2)
-> [HTTP node2] handle_request_vote()
-> [node1 RPC worker] _become_leader_locked()
-> [node1 timer] _broadcast_append_entries()
-> [node1 RPC worker] _replicate_to_peer(node2)
-> [HTTP node2] handle_append_entries()
-> [node1 процесс остановлен]
-> [node2 timer] _timer_loop()
-> [node2 timer] _start_election(term=2)
-> [node2 RPC worker] _request_vote_from_peer(node3)
-> [HTTP node3] handle_request_vote()
-> [node2 RPC worker] _become_leader_locked()
-> [node2 timer] _broadcast_append_entries()
```

