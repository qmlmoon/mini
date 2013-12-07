/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.dima.minidb.test.io.manager.benchmark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

/**
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 */
@RunWith(Parameterized.class)
public class BufferBenchmark extends AbstractBenchmark {

	private static enum BufferAllocationStrategy {
		MANAGED, ALLOCATE, DIRECT
	}

	private static enum ChannelWritingStrategy {
		SINGLE, BULK
	}

	private static final int TOTAL_SIZE = 1024 * 1024; // 1 MB

	private static final int BENCHMARK_ROUNDS = 5;

	private static final int WARMUP_ROUNDS = 0;

	private final BufferAllocationStrategy bufferAllocationStrategy;

	private final ChannelWritingStrategy channelWritingStrategy;

	public BufferBenchmark(BufferAllocationStrategy bufferAllocationStrategy, ChannelWritingStrategy channelWritingStrategy) {
		this.bufferAllocationStrategy = bufferAllocationStrategy;
		this.channelWritingStrategy = channelWritingStrategy;
		System.out.println(String.format("Running tests with [%s, %s]", bufferAllocationStrategy.name(), channelWritingStrategy.name()));
	}

	@Parameters
	public static Collection<Object[]> testParameters() {
		Collection<Object[]> parameters = new LinkedList<Object[]>();
		for (BufferAllocationStrategy bufferAllocationStrategy : BufferAllocationStrategy.values()) {
			for (ChannelWritingStrategy channelWritingStrategy : ChannelWritingStrategy.values()) {
				parameters.add(new Object[] { bufferAllocationStrategy, channelWritingStrategy });
			}
		}
		return parameters;
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile0004K() throws FileNotFoundException, IOException {
		writeToFile(4096);
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile0008K() throws FileNotFoundException, IOException {
		writeToFile(8192);
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile0016K() throws FileNotFoundException, IOException {
		writeToFile(16384);
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile0064K() throws FileNotFoundException, IOException {
		writeToFile(65536);
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile0256K() throws FileNotFoundException, IOException {
		writeToFile(262144);
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
	public void writeToFile1024K() throws FileNotFoundException, IOException {
		writeToFile(1048576);
	}

	private void writeToFile(final int bufferSize) throws IOException {
		final int N = TOTAL_SIZE / bufferSize;

		Path filePath = Files.createTempFile(String.format("bpm_benchmark.allocate.%08d.", bufferSize), ".tmp");

		RandomAccessFile file = new RandomAccessFile(filePath.toString(), "rwd");
		FileChannel channel = file.getChannel();

		try {
			if (this.bufferAllocationStrategy == BufferAllocationStrategy.MANAGED) {
				if (this.channelWritingStrategy == ChannelWritingStrategy.SINGLE) {
					byte[] managed = new byte[bufferSize];
					for (int i = 0; i < N; i++) {
						channel.write(ByteBuffer.wrap(managed));
					}
				}
				if (this.channelWritingStrategy == ChannelWritingStrategy.BULK) {
					byte[][] managed = new byte[N][bufferSize];
					for (int i = 0; i < N; i++) {
						managed[i] = new byte[bufferSize];
					}
					ByteBuffer[] buffers = new ByteBuffer[N];
					for (int i = 0; i < N; i++) {
						buffers[i] = ByteBuffer.wrap(managed[i]);
					}
					channel.write(buffers);
				}
			}
			if (this.bufferAllocationStrategy == BufferAllocationStrategy.ALLOCATE) {
				if (this.channelWritingStrategy == ChannelWritingStrategy.SINGLE) {
					for (int i = 0; i < N; i++) {
						channel.write(ByteBuffer.allocate(bufferSize));
					}
				}
				if (this.channelWritingStrategy == ChannelWritingStrategy.BULK) {
					ByteBuffer[] buffers = new ByteBuffer[N];
					for (int i = 0; i < N; i++) {
						buffers[i] = ByteBuffer.allocate(bufferSize);
					}
					channel.write(buffers);
				}
			}
			if (this.bufferAllocationStrategy == BufferAllocationStrategy.DIRECT) {
				if (this.channelWritingStrategy == ChannelWritingStrategy.SINGLE) {
					for (int i = 0; i < N; i++) {
						channel.write(ByteBuffer.allocateDirect(bufferSize));
					}
				}
				if (this.channelWritingStrategy == ChannelWritingStrategy.BULK) {
					ByteBuffer[] buffers = new ByteBuffer[N];
					for (int i = 0; i < N; i++) {
						buffers[i] = ByteBuffer.allocateDirect(bufferSize);
					}
					channel.write(buffers);
				}
			}
		} finally {
			channel.close();
			file.close();
		}
	}
}
