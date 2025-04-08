<img src="./docs/assets/logo.png" alt="Qsync logo" width="500px" />

# Простая и эффективная распределенная очередь задач в Go

**Qsync** — это библиотека Go для постановки задач в очередь и их асинхронной обработки с помощью обработчиков.

Краткий обзор работы:
- Клиент ставит задачи в очередь.
- Сервер извлекает задачи из очередей и запускает рабочую процедуру для каждой задачи.
- Задачи обрабатываются одновременно несколькими работниками.

## Установка

```sh
go get -u github.com/alnovi/qsync
```

## Использование

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/alnovi/qsync"
    "github.com/redis/go-redis/v9"
)

var handleFn = func(ctx context.Context, task *qsync.TaskInfo) error {
    fmt.Println(task.Id, string(task.Payload))
    return nil
}

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    options := []qsync.Option{
        qsync.WithPrefix("alnovi"),
    }

    queue, err := qsync.New(client, options...)
    must(err)

    mux := qsync.NewMux()
    must(mux.HandleFunc("reindex-file", handleFn))

    must(queue.Start(mux))

    defer queue.Stop()

    task := qsync.NewTask("reindex-file", []byte("task payload"),
        qsync.WithRetry(3),
        qsync.WithDelay(30 * time.Second),
        qsync.WithRetryDelay(5 * time.Second),
    )

    err = queue.Enqueue(context.Background(), qsync.Default, task)
    must(err)

    time.Sleep(time.Minute)
}

func must(err error) {
    if err != nil {
        panic(err)
    }
}
```
