package com.whz.logcollector.store.util;

import com.sun.jna.*;

/**
 * @author whz
 * @date 2022/1/16 20:53
 **/
public interface CUtil extends Library {
    CUtil INSTANCE = Native.load(Platform.isWindows() ? "msvcrt" : "c", CUtil.class);

    int MADV_WILLNEED = 3;


    int mlock(Pointer var1, NativeLong var2);

    int munlock(Pointer var1, NativeLong var2);

    int madvise(Pointer var1, NativeLong var2, int var3);

}
