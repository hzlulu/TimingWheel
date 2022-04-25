package com.codecopyer.timeWheel;

import java.util.Date;

/**
 * @author guest
 */
public class DelayedOperation extends TimerTask {

    public DelayedOperation(long delayMs) {
        super.delayMs = delayMs;
    }

    @Override
    public void run() {
        System.out.println(" do the job" + new Date());
    }
}
