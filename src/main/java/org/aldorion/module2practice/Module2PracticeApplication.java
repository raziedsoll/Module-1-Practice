package org.aldorion.module2practice;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class Module2PracticeApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(Module2PracticeApplication.class)
                .profiles("moderation")
                .run(args);
    }
}