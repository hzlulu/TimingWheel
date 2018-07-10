package com.codecopyer.timeWheel;

public class SystemTimerTest {
    public static void main(String[] args) {
        SystemTimer systemTimer = new SystemTimer("timer");
        System.out.println(System.currentTimeMillis());
        for (int i = 0; i < 100; i++) {
            systemTimer.add(new DelayedOperation(500+i));
        }
        System.out.println(System.nanoTime());
        boolean flag = true;
        while (flag) {
            boolean b = systemTimer.advanceClock(200);
//            flag = b;
        }
    }
}
