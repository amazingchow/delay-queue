package delayqueue

import (
	"sync"

	"github.com/vmihailenco/msgpack"

	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

type Task struct {
	Topic string `json:"topic" msgpack:"1"` // 任务类型, 可以是具体的业务名称
	Id    string `json:"id" msgpack:"2"`    // 任务唯一标识, 用来检索/删除指定的任务
	Delay int64  `json:"delay" msgpack:"3"` // 任务需要延迟执行的时间, 单位: 秒
	TTR   int64  `json:"ttr" msgpack:"4"`   // 任务执行超时的时间, 单位: 秒
	Blob  string `json:"blob" msgpack:"5"`  // 任务内容, 供消费者做具体的业务处理, 以json格式存储
}

/*
	key -> task id
*/

/*
	Improvement:

	通常我们讨论的redis并发读写问题是针对同一个key的, 由于不同的task使用不同的key来存储,
	因此将所有task的读写命令都串行化, 是一种效率非常低下的做法, 势必会影响整体的吞吐性能.
	优化并发控制粒度, 将并发竞争控制限定在单个key上.
*/

type TaskRWController struct {
	mu          sync.Mutex
	LookupTable map[string]*TaskRWControlView
}

func NewTaskRWController() *TaskRWController {
	return &TaskRWController{
		LookupTable: make(map[string]*TaskRWControlView),
	}
}

func (ctrl *TaskRWController) PutTask(inst *redis.RedisConnPoolSingleton, key string, task *Task, debug bool) error {
	ctrl.mu.Lock()
	view, exist := ctrl.LookupTable[key]
	if !exist {
		// 视图不存在, 新增并发控制视图
		view = NewTaskRWControlView(key)
		ctrl.LookupTable[key] = view
	}
	ctrl.mu.Unlock()
	return view.putTask(inst, task, debug)
}

func (ctrl *TaskRWController) GetTask(inst *redis.RedisConnPoolSingleton, key string, debug bool) (*Task, error) {
	ctrl.mu.Lock()
	view, exist := ctrl.LookupTable[key]
	if !exist {
		// 视图不存在, 直接返回
		ctrl.mu.Unlock()
		return nil, nil
	}
	ctrl.mu.Unlock()
	return view.getTask(inst, debug)
}

func (ctrl *TaskRWController) DelTask(inst *redis.RedisConnPoolSingleton, key string, debug bool) error {
	ctrl.mu.Lock()
	view, exist := ctrl.LookupTable[key]
	if !exist {
		// 视图不存在, 直接返回
		ctrl.mu.Unlock()
		return nil
	}
	delete(ctrl.LookupTable, key)
	ctrl.mu.Unlock()
	return view.delTask(inst, debug)
}

type TaskRWControlView struct {
	mu  sync.Mutex
	key string
}

func NewTaskRWControlView(key string) *TaskRWControlView {
	return &TaskRWControlView{
		key: key,
	}
}

func (view *TaskRWControlView) putTask(inst *redis.RedisConnPoolSingleton, task *Task, debug bool) error {
	view.mu.Lock()
	defer view.mu.Unlock()

	v, err := msgpack.Marshal(task)
	if err != nil {
		return err
	}
	_, err = redis.ExecCommand(inst, debug, "SET", view.key, v)
	return err
}

func (view *TaskRWControlView) getTask(inst *redis.RedisConnPoolSingleton, debug bool) (*Task, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	v, err := redis.ExecCommand(inst, debug, "GET", view.key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	task := Task{}
	if err = msgpack.Unmarshal(v.([]byte), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (view *TaskRWControlView) delTask(inst *redis.RedisConnPoolSingleton, debug bool) error {
	view.mu.Lock()
	defer view.mu.Unlock()

	_, err := redis.ExecCommand(inst, debug, "DEL", view.key)
	return err
}
