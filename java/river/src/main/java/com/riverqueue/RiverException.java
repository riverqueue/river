package com.riverqueue;

/** A failure in River validation, storage, or execution. */
public class RiverException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final Code code;

  public RiverException(Code code, String message) {
    super(message);
    this.code = code;
  }

  public RiverException(Code code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public Code code() {
    return code;
  }

  /** Stable categories suitable for application error handling. */
  public enum Code {
    DATABASE,
    NOT_FOUND,
    REJECTED,
    UNSUPPORTED
  }
}
