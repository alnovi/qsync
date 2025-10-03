# Простая и эффективная распределенная очередь задач в Go

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/alnovi/qsync)
![GitHub License](https://img.shields.io/github/license/alnovi/qsync)
[![Go Report Card](https://goreportcard.com/badge/github.com/alnovi/qsync)](https://goreportcard.com/report/github.com/alnovi/qsync)
![GitHub top language](https://img.shields.io/github/languages/top/alnovi/qsync)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/alnovi/qsync)
![GitHub Release](https://img.shields.io/github/v/release/alnovi/qsync)

**Qsync** — это библиотека Go для постановки задач в очередь и их асинхронной обработки с помощью обработчиков.

Краткий обзор работы:
- Клиент ставит задачи в очередь.
- Сервер извлекает задачи из очередей и запускает рабочую процедуру для каждой задачи.
- Задачи обрабатываются одновременно несколькими работниками.

Очереди задач используются как механизм распределения работы между несколькими машинами.
Система может состоять из нескольких рабочих серверов, обеспечивая высокую доступность и горизонтальное масштабирование.

## Установка

```sh
go get -u github.com/alnovi/qsync
```

## Функции

- Гарантировано хотя бы одно выполнение задачи.
- Планирование задач.
- Повторные попытки неудачных задач.
- Автоматическое восстановление задач в случае сбоя работника.
- Очереди с взвешенным приоритетом.
- Низкая задержка при добавлении задачи, поскольку запись в Redis выполняется быстро.
- Дедупликация задач с использованием уникальной опции.
- Разрешить тайм-аут и крайний срок для каждой задачи.
- Совместимость с кластером Redis 

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
