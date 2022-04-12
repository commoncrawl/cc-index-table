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
package org.commoncrawl.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TestIndexTableBase {

	private static final Logger LOG = LoggerFactory.getLogger(TestIndexTableBase.class);

	protected SparkSession getSession() {
		return SparkSession.builder().master("local[*]").config(new SparkConf()).getOrCreate();
	}

	protected void testSingleRow(Row row, StructType schema) throws IOException {
		List<Row> table = new ArrayList<Row>();
		table.add(row);
		assertEquals(schema.fields().length, row.length());
		Dataset<Row> df = getSession().createDataFrame(table, schema);
		df.printSchema();
		validateSchemaFirstRow(df, schema);
	}

	protected void validateSchemaFirstRow(Dataset<Row> df, StructType schema) {
		assertEquals(1, df.count());
		df.show();
		Row row = df.first();
		assertNotNull(row);
		validateSchemaRow(row, schema);
	}

	protected void validateSchemaRow(Row row, StructType schema) {
		for (int i = 0; i < row.length(); i++) {
			Object val = row.get(i);
			StructField field = schema.fields()[i];
			validateField(i, val, field, "");
		}
	}

	protected void validateField(int i, Object val, StructField field, String indentation) {
		if (val == null) {
			if (!field.nullable()) {
				fail("Field " + field.name() + " is null but not nullable");
			}
		} else if (field.dataType() instanceof StructType) {
			LOG.info("{}{}\t(nested)\t{}", indentation, i, field.name());
			StructType f = ((StructType) field.dataType());
			Row r = ((Row) val);
			StructField[] fields = f.fields();
			for (int j = 0; j < r.length(); j++) {
				Object v = r.get(j);
				validateField(j, v, fields[j], indentation + "\t");
			}
		} else {
			LOG.info("{}{}\t{}\t{}\t{}", indentation, i, field.name(), field.dataType(), val);
		}
	}

}
