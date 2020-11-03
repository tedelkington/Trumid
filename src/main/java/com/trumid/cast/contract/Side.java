package com.trumid.cast.contract;

public enum Side {
    Buy(1),
    Sell(2);

    private final int fixValue;

    Side(int fixValue) {
        this.fixValue = fixValue;
    }

    public int fixValue() {
        return fixValue;
    }

    public static Side fromFix(int fixValue) {
        switch (fixValue) {
            case 1:
                return Buy;

            case 2:
                return Sell;

            default:
                throw new IllegalArgumentException("Invalid side value of " + fixValue);
        }
    }
}
