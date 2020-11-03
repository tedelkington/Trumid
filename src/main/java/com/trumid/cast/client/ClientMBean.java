package com.trumid.cast.client;

public interface ClientMBean {

    String sendCast(int originatorUserId, int bondId, int side);

    String cancelCast(int originatorUserId, int bondId, int side);

    String getActiveCasts(int targetUserId);
}
