/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.hyper.hive;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandlerUtils.class);

  public static void addDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    jars.addAll(conf.getStringCollection("tmpjars"));
    for (Class<?> clazz : classes) {
      if (clazz == null) {
        continue;
      }
      String path = jarFinderGetJar(clazz);
      if (path == null) {
        throw new RuntimeException(
                "Could not find jar for class " + clazz + " in order to ship it to the cluster.");
      }
      if (!localFs.exists(new Path(path))) {
        throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  public static String jarFinderGetJar(Class klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
      try {
        for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
          URL url = (URL) itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

}
