package com.huawei.hwcloud.tarus.kvstore.store.example;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreCheck;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class KVStoreServer implements KVStoreCheck {
	
	private static final Logger log = LoggerFactory.getLogger(KVStoreServer.class);

	@Override
	public double execute() {
		String dir = ResourceManager.buildFullDir(ConfigManager.DATA_FILE_DIR);
		double ret = 0.1;

		RpcProcess rpcProcess = new RpcProcess();
		rpcProcess.init(dir);
		TcpServer.init(rpcProcess);

		return ret;
	}
}
