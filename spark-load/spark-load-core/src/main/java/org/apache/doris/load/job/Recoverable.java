package org.apache.doris.load.job;

import org.apache.doris.common.SparkLoadException;

public interface Recoverable {

    boolean canBeRecovered() throws SparkLoadException;

    void prepareRecover() throws SparkLoadException;

}
