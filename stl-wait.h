#ifndef __STL_WAIT_H__
#define __STL_WAIT_H__

/* modified from include/linux/wait.h - wait on a condition with
 * irqsave / irqrestore
 */

#define __wait_event_lock_irqsave(wq, condition, lock, flags)           \
        (void)___wait_event(wq, condition, TASK_UNINTERRUPTIBLE, 0, 0,  \
                            spin_unlock_irqrestore(&lock, flags);       \
                            schedule();                                 \
                            spin_lock_irqsave(&lock, flags))


#define wait_event_lock_irqsave(wq, condition, lock, flags)             \
do {                                                                    \
        if (condition)                                                  \
                break;                                                  \
        __wait_event_lock_irqsave(wq, condition, lock, flags);          \
} while (0)

#endif
