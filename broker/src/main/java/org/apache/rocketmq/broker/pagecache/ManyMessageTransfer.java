/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.pagecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.rocketmq.store.GetMessageResult;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer       byteBufferHeader;
    private final GetMessageResult getMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long                   transferred;

    public ManyMessageTransfer(final ByteBuffer byteBufferHeader,
                               final GetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
    }

    @Override
    public long position() {
        int pos = this.byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return this.transferred;
    }

    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.getMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(final WritableByteChannel target,
                           final long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            this.transferred += target.write(this.byteBufferHeader);
            return this.transferred;
        } else {
            List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    this.transferred += target.write(bb);
                    return this.transferred;
                }
            }
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.getMessageResult.release();
    }

    /********************* 4.1.x netty modify *******************/
    
    /**
     * @see io.netty.channel.FileRegion#transferred()
     */
    @Override
    public long transferred() {
        return 0;
    }

    /**
     * @see io.netty.channel.FileRegion#retain()
     */
    @Override
    public FileRegion retain() {
        return null;
    }

    /**
     * @see io.netty.channel.FileRegion#retain(int)
     */
    @Override
    public FileRegion retain(final int increment) {
        return null;
    }

    /**
     * @see io.netty.channel.FileRegion#touch()
     */
    @Override
    public FileRegion touch() {
        return null;
    }

    /**
     * @see io.netty.channel.FileRegion#touch(java.lang.Object)
     */
    @Override
    public FileRegion touch(final Object hint) {
        return null;
    }
}
