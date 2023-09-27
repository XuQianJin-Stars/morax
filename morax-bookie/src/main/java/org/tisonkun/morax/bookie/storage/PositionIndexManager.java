/*
 * Copyright 2023 tison <wander4096@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tisonkun.morax.bookie.storage;

import org.tisonkun.morax.proto.bookie.EntryLocation;

import java.io.Closeable;
import java.io.IOException;

public abstract class PositionIndexManager implements Closeable {
    public abstract void addPosition(long ledgerId, long entryId, int logId, long position) throws IOException;

    public abstract EntryLocation findPosition(long ledgerId, long entryId) throws IOException;
}
