package org.corfudb.router.multiServiceTest;

/**
 * Created by mwei on 12/5/16.
 */
public class WrongPasswordException extends RuntimeException {

    WrongPasswordException() {
        super("Wrong password!");
    }
}
