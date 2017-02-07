package com.getindata.subsession;

import com.getindata.UserEvent;

import java.util.List;

@FunctionalInterface
public interface SubSessionProcessor {
    void processSubSession(final List<UserEvent> events);
}

