package ar.edu.itba;

public enum Status {
    NO_DATA, EMPLOYED, UNEMPLOYED, INACTIVE;

    public static Status fromInt(int intValue) {
        switch(intValue) {
            case 0:
                return NO_DATA;
            case 1:
                return EMPLOYED;
            case 2:
                return UNEMPLOYED;
            case 3:
                return INACTIVE;
            default:
                throw new IllegalArgumentException("Unknown numerical value " + intValue);
        }
    }
}
