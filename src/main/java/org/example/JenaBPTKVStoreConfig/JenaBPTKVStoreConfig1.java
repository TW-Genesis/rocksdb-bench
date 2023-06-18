package org.example.JenaBPTKVStoreConfig;

import org.example.CommonKVStoreConfig.CommonKVStoreConfig;

public class JenaBPTKVStoreConfig1 implements JenaBPTKVStoreConfig{
    private final CommonKVStoreConfig commonKVStoreConfig;
    public JenaBPTKVStoreConfig1(CommonKVStoreConfig commonKVStoreConfig) {
        this.commonKVStoreConfig = commonKVStoreConfig;
    }

    @Override
    public int getKVSize() {
        return commonKVStoreConfig.getKVSize();
    }
}
