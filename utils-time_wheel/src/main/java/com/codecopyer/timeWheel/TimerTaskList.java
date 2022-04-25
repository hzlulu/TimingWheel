package com.codecopyer.timeWheel;


import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


/**
 * @author guest
 */
@ThreadSafe
class TimerTaskList implements Delayed {


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
     * Set the bucket's expiration time
     *
     * @return true if the expiration time is changed
     */
    public boolean setExpiration(Long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    /**
     * Get the bucket's expiration time
     *
     * @return expiration time
     */
    public Long getExpiration() {
        return expiration.get();
    }

    /**
     * Apply the supplied function to each of tasks in this list
     */
    public void foreach(Function<TimerTask, Void> f) {
        synchronized (this) {
            TimerTaskEntry entry = root.next;
            while (entry != root) {
                TimerTaskEntry nextEntry = entry.next;

                if (!entry.cancelled()) {
                    f.apply(entry.getTimerTask());
                }

                entry = nextEntry;
            }
        }
    }

    /**
     * Add a timer task entry to this list
     */
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

    /**
     * Remove the specified timer task entry from this list
     */
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

    /**
     * Remove all task entries and apply the supplied function to each of them
     */
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
        return Long.compare(getExpiration(), other.getExpiration());
    }
}
