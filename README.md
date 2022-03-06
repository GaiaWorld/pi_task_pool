# pi_task_pool

任务池

可以向任务池中插入不同优先级的任务，任务池提供弹出功能，任务池大概率会弹出优先级高的任务。

任务池支持的任务可以大致分为两类：

### 队列任务：插入队列任务需要先创建队列，插入到同一个队列的任务，会按顺序弹出。即便一个优先级很高的任务，

如果它所在的队列头部还存在任务，也需要等待这些任务弹出后才能被弹出。

尽管队列任务的优先级在本队列中并不生效，但是可以提高整个队列的优先级。如果向一个队列插入一个优先级很高的任务，接下来，弹出该队列头部的任务的概率会变高。

### 单例任务：在任务池中，如果不是队列任务，那一定是一个单例任务。

单列任务与队列任务的区别是，单例任务不需要排队，单例任务的优先级越高，弹出的概率越大。

尽管任务池中的任务仅分为两类（队列任务，单例任务），但每类任务又可以分为可删除的任务、和不可删除的任务。

一些任务在弹出前，如果不会被取消，推荐插入不可删任务。不可删除的任务在任务池内部使用了更高效的数据结构。

除此以外，任务池还可以插入一个延时的任务，该任务先被缓存在定时器中，超时后，才能有机会被弹出

