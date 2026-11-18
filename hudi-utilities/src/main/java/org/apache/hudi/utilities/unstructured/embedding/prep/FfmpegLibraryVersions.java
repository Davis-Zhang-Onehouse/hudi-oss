/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * The FFmpeg libraries (JavaCV and its FFmpeg preset) this Hudi build compiles and tests against: the {@code javacv.version} and
 * {@code javacv.ffmpeg.version} of the build. They are provided, never bundled, and a deployment adds them to the executors only,
 * so the driver that pins the prep logic version usually has none of them. The video and audio prep logic versions therefore
 * name these declared versions, never what a class path holds, and an executor about to decode with FFmpeg checks that the jars
 * it loaded are these versions ({@link #requireDeclared}).
 */
public final class FfmpegLibraryVersions {

  /** JavaCV, as {@code groupId/artifactId:version}. */
  public static final String JAVACV = "org.bytedeco/javacv:1.5.11";
  /** JavaCV's FFmpeg preset, as {@code groupId/artifactId:version}. */
  public static final String FFMPEG = "org.bytedeco/ffmpeg:7.1-1.5.11";

  private static final String JAVACV_CLASS = "org/bytedeco/javacv/FFmpegFrameGrabber.class";
  private static final String FFMPEG_CLASS = "org/bytedeco/ffmpeg/global/avutil.class";

  private FfmpegLibraryVersions() {
  }

  /**
   * Refuses FFmpeg jars that are not the declared versions, before the first video or audio track is decoded with them: what
   * they would prepare is not what the prep logic version pinned on the table says.
   *
   * @param loader the class loader the FFmpeg classes are resolved through
   * @throws PreprocessorConfigurationException naming each expected and found version; the task fails and nothing is prepared
   */
  public static void requireDeclared(ClassLoader loader) {
    List<String> mismatches = new ArrayList<>();
    for (String[] library : new String[][] {{JAVACV_CLASS, JAVACV}, {FFMPEG_CLASS, FFMPEG}}) {
      String found = loaded(loader, library[0], library[1]);
      if (!library[1].equals(found)) {
        mismatches.add("expected " + library[1] + ", found " + found);
      }
    }
    if (!mismatches.isEmpty()) {
      throw new PreprocessorConfigurationException("The FFmpeg libraries on this executor are not the versions this Hudi build"
          + " declares and its prep logic version names (" + String.join("; ", mismatches) + "); add the declared versions'"
          + " jars to the job; nothing was prepared with FFmpeg", null);
    }
  }

  /** The {@code groupId/artifactId:version} of the jar {@code loader} finds {@code resource} in, from its Maven metadata. */
  static String loaded(ClassLoader loader, String resource, String declared) {
    String artifact = declared.substring(0, declared.indexOf(':'));
    URL url = loader.getResource(resource);
    if (url == null) {
      return "absent";
    }
    try {
      URLConnection connection = url.openConnection();
      if (!(connection instanceof JarURLConnection)) {
        return "no " + artifact + " Maven metadata in " + url;
      }
      JarFile jar = ((JarURLConnection) connection).getJarFile();
      JarEntry entry = jar.getJarEntry("META-INF/maven/" + artifact + "/pom.properties");
      if (entry == null) {
        return "no " + artifact + " Maven metadata in " + jar.getName();
      }
      Properties metadata = new Properties();
      try (InputStream in = jar.getInputStream(entry)) {
        metadata.load(in);
      }
      return artifact + ":" + metadata.getProperty("version", "").trim();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read the version of " + artifact + " from " + url, e);
    }
  }
}
