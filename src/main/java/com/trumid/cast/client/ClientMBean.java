package com.trumid.cast.client;

public interface ClientMBean {

    String sendCast(int originatorUserId, int bondId, int side);

    String sendCast(int originatorUserId, int bondId, int side, String targetedUserIds); // seems to me like this is the intended interface..?

    String cancelCast(int originatorUserId, int bondId, int side);

    String getActiveCasts(int targetUserId);
}
