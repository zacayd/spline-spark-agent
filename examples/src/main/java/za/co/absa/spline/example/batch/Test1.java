/*
 * Copyright 2017 ABSA Group Limited
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

package za.co.absa.spline.example.batch;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import za.co.absa.spline.agent.AgentConfig;
import za.co.absa.spline.harvester.SparkLineageInitializer;

import java.io.InputStream;

public class Test1 {

    public static void main(String[] args) {
        InputStream in = JavaExampleJob.class.getClassLoader().getResourceAsStream("spline.default.yaml");
        if (in == null) {
            System.out.println("🚨 spline.default.yaml NOT FOUND in classpath!");
        } else {
            System.out.println("✅ spline.default.yaml FOUND in classpath!");
        }
    }
}
