package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// ЗАДАНИЕ:
// * сделать из плохого кода хороший;
// * важно сохранить логику появления ошибочных тасков;
// * сделать правильную мультипоточность обработки заданий.
// Обновленный код отправить через merge-request.

// приложение эмулирует получение и обработку тасков, пытается и получать и обрабатывать в многопоточном режиме
// В конце должно выводить успешные таски и ошибки выполнены остальных тасков

// A Ttype represents a meaninglessness of our life
type Ttype struct {
	id int
	// Поменять на time? Сделаем вид что такски приходят по сети и их нужно будет парсить.
	cT         string // время создания
	fT         string // время выполнения
	isValid    bool
	taskRESULT []byte
}

type taskSorter func(t Ttype, done chan struct{})

type taskWorker func(t Ttype) Ttype

// Небольшой воркер предстовляющий из себе пайплайн(in -> work -> sort)
type Worker struct {
	ctx context.Context

	in   chan Ttype
	done chan struct{}

	sorter taskSorter
	worker taskWorker
}

func NewWorker(ctx context.Context, in chan Ttype, sorter taskSorter, wworker taskWorker) *Worker {
	return &Worker{
		ctx:    ctx,
		in:     in,
		sorter: sorter,
		worker: wworker,

		done: make(chan struct{}),
	}
}

func (w *Worker) sort(in <-chan Ttype) {
	go func() {
		w.sorter(<-in, w.done)
	}()
}

func (w *Worker) work(t Ttype) <-chan Ttype {
	out := make(chan Ttype)
	go func() {
		out <- w.worker(t)
		close(out)
	}()
	return out
}

func (w *Worker) StartWorker() {
	go func() {
		log.Println("start worker")
		for {
			select {
			case <-w.done:
				return
			case ttype, ok := <-w.in:
				if ok {
					w.sort(w.work(ttype))
				}
			}
		}
	}()
}

func (w *Worker) StopWorker() {
	close(w.done)
}

func main() {
	taskCreater := func(a chan Ttype, ctx context.Context) {
		go func() {
			// Нужно позаботится о закрытии канала.
			defer close(a)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					ft := time.Now().Format(time.RFC3339)
					if time.Now().Nanosecond()%2 > 0 { // вот такое условие появления ошибочных тасков
						ft = "Some error occured"
					}
					a <- Ttype{cT: ft, id: int(time.Now().Unix())} // передаем таск на выполнение
				}

			}
		}()
	}

	superChan := make(chan Ttype, 10)
	go taskCreater(superChan, context.TODO())

	// Не понятно зачем хранить такой резулт с учетом того, что он нужен только для сортировки.
	taskWorker := func(a Ttype) Ttype {
		// Можно не проверять ошибку т.к tt.After(time.Now().Add(-20 * time.Second)) от tt по умолчанию нас устраивает, да будет лишняя операция.
		tt, _ := time.Parse(time.RFC3339, a.cT)
		a.isValid = tt.After(time.Now().Add(-20 * time.Second))
		a.fT = time.Now().Format(time.RFC3339Nano)
		time.Sleep(time.Millisecond * 150)
		return a
	}

	doneTasks := make(chan Ttype)
	undoneTasks := make(chan error)

	// Для проверки приходиться сравнивать строки, проще сделать флаг и сравнивать 1 байт.
	taskSorter := func(t Ttype, done chan struct{}) {
		// При частых тасках может возникнуть ситуации, что каналы закроются раньше, чем все данные попадут сюда.
		select {
		case <-done:
			// Проше всего закрыть каналы тут.
			close(doneTasks)
			close(undoneTasks)
			return
		default:
			if t.isValid {
				doneTasks <- t
			} else {
				undoneTasks <- fmt.Errorf("task id %d time %s, error %s", t.id, t.cT, t.taskRESULT)
			}
		}

	}

	result := map[int]Ttype{}
	err := []error{}

	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range doneTasks {
				result[t.id] = t
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range undoneTasks {
				err = append(err, t)
			}
		}()

	}()

	// Стартуем воркера.
	worker := NewWorker(context.Background(), superChan, taskSorter, taskWorker)
	worker.StartWorker()

	// Эмитируем полезную нагрузку.
	time.Sleep(time.Second * 30)

	worker.StopWorker()

	// Ждем пока допишут ответы.
	wg.Wait()
	println("Errors:")
	for _, v := range err {
		fmt.Println(v)
	}
	println("Done tasks:")
	for k := range result {
		fmt.Println(k)
	}
}
