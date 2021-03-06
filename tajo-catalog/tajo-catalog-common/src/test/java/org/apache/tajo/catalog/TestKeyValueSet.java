/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestKeyValueSet {
	@Test
	public final void testPutAndGet() {
		KeyValueSet opts = new KeyValueSet();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		assertEquals(",", opts.get("delimiter"));
		assertEquals("abc", opts.get("name"));
	}

	@Test
	public final void testGetProto() {		
		KeyValueSet opts = new KeyValueSet();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		PrimitiveProtos.KeyValueSetProto proto = opts.getProto();
		KeyValueSet opts2 = new KeyValueSet(proto);
		
		assertEquals(opts, opts2);
	}
	
	@Test
	public final void testDelete() {
		KeyValueSet opts = new KeyValueSet();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		assertEquals("abc", opts.get("name"));
		assertEquals("abc", opts.delete("name"));
		assertNull(opts.get("name"));
		
		KeyValueSet opts2 = new KeyValueSet(opts.getProto());
		assertNull(opts2.get("name"));
	}
}
