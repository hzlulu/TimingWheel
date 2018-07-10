package com.codecopyer.timeWheel;


import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class TimerTaskList implements Delayed{


    private AtomicInteger taskCounter;
    /**
     * 哨兵节点
     */
    private TimerTaskEntry root;

    private AtomicLong expiration;

    public TimerTaskList() {
    }

    public TimerTaskList(AtomicInteger taskCounter) {
        // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
        // root.next points to the head
        // root.prev points to the tail
        this.taskCounter = taskCounter;
        this.root = new TimerTaskEntry(null, -1L);
        this.root.next = root;
        this.root.prev = root;
        this.expiration = new AtomicLong(-1L);
    }

    /**
     * 设置bucket到期时间
     *
     * @param expirationMs 到期时间
     * @return true 如果到期时间被改变
     */
    public boolean setExpiration(Long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    /**
     * 获取bucket到期时间
     *
     * @return 到期时间
     */
    public Long getExpiration() {
        return expiration.get();
    }

    /**
     * 将提供的函数应用于此列表中的每个任务
     *
     * @param f 需要执行的函数
     */
    public synchronized void foreach(Function<TimerTask, Void> f) {
        TimerTaskEntry entry = root.next;
        while (entry != root) {
            TimerTaskEntry nextEntry = entry.next;

            if (!entry.cancelled()) {
                f.apply(entry.getTimerTask());
            }

            entry = nextEntry;
        }
    }

    // Add a timer task entry to this list
    public void add(TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            // Remove the timer task entry if it is already in any other list
            // We do this outside of the sync block below to avoid deadlocking.
            // We may retry until timerTaskEntry.list becomes null.
            timerTaskEntry.remove();

            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.getList() == null) {
                        // put the timer task entry to the end of the list. (root.prev points to the tail entry)
                        TimerTaskEntry tail = root.prev;
                        timerTaskEntry.next = root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.setList(this);
                        tail.next = timerTaskEntry;
                        root.prev = timerTaskEntry;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    // Remove the specified timer task entry from this list
    public void remove(TimerTaskEntry timerTaskEntry) {
        synchronized (this) {
            synchronized (timerTaskEntry) {
                if (timerTaskEntry.getList() == this) {
                    timerTaskEntry.next.prev = timerTaskEntry.prev;
                    timerTaskEntry.prev.next = timerTaskEntry.next;
                    timerTaskEntry.next = null;
                    timerTaskEntry.prev = null;
                    timerTaskEntry.setList(null);
                    taskCounter.decrementAndGet();
                }
            }
        }
    }

    // Remove all task entries and apply the supplied function to each of them
    public void flush(Function<TimerTaskEntry, Void> f) {
        synchronized (this) {
            TimerTaskEntry head = root.next;
            while (head != root) {
                remove(head);
                f.apply(head);
                head = root.next;
            }
            expiration.set(-1L);
        }
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(Long.max(getExpiration() - Time.getHiresClockMs(), 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed d) {
        TimerTaskList other;
        if (d instanceof TimerTaskList) {
            other = (TimerTaskList) d;
        } else {
            throw new ClassCastException("can not cast to TimerTaskList");
        }

        if (getExpiration() < other.getExpiration()) {
            return -1;
        } else if (getExpiration() > other.getExpiration()) {
            return 1;
        } else {
            return 0;
        }
    }
}
