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

package org.apache.hudi.utilities.unstructured.embedding.text;

/**
 * The hard limits of one chunk: {@code fits(s)} holds when {@code s} has at most
 * {@code windowChars} UTF-16 units, at most {@code maxWords} words and at most {@code maxTokens} tokens of the
 * configured tokenizer, checked in that order so the tokenizer never sees more than {@code windowChars} chars.
 * Every tokenizer call goes through this class: a {@link RuntimeException} from the tokenizer becomes the caller's
 * {@link PackFailures.Kind#TOKENIZER_ERROR} failure, built by its {@link PackFailures}.
 */
public final class TokenBudget {

  private final Tokenizer tokenizer;
  private final int maxTokens;
  private final int maxWords;
  private final int windowChars;
  private final PackFailures failures;

  /**
   * A budget whose failures (an internal limit breach, a tokenizer failure) are built by {@code failures}.
   *
   * @param maxTokens   tokens of one chunk at most
   * @param maxWords    words of one chunk at most
   * @param windowChars UTF-16 units of one chunk at most; the tokenizer never sees a longer input
   */
  public TokenBudget(Tokenizer tokenizer, int maxTokens, int maxWords, int windowChars, PackFailures failures) {
    this.tokenizer = tokenizer;
    this.maxTokens = maxTokens;
    this.maxWords = maxWords;
    this.windowChars = windowChars;
    this.failures = failures;
  }

  /** Tokens of one chunk at most. */
  public int maxTokens() {
    return maxTokens;
  }

  /** Words of one chunk at most. */
  public int maxWords() {
    return maxWords;
  }

  /** UTF-16 units of one chunk at most; the tokenizer never sees a longer input. */
  public int windowChars() {
    return windowChars;
  }

  /** The caller's failure of {@code kind}, to be thrown. */
  RuntimeException failure(PackFailures.Kind kind, String message) {
    return failures.failure(kind, message, null);
  }

  /** Whether {@code s} is within the window, the word limit and the token limit, checked in that order. */
  public boolean fits(String s) {
    return s.length() <= windowChars && words(s) <= maxWords && count(s) <= maxTokens;
  }

  /** Tokens of {@code s}; {@code s} must not be longer than the window. */
  public int count(String s) {
    if (s.length() > windowChars) {
      throw failure(PackFailures.Kind.INTERNAL, "tokenizer input of " + s.length() + " chars exceeds the window of "
          + windowChars);
    }
    try {
      return tokenizer.countTokens(s);
    } catch (RuntimeException e) {
      throw failures.failure(PackFailures.Kind.TOKENIZER_ERROR, "tokenizer " + tokenizer.id() + " failed", e);
    }
  }

  /** The tokenizer's own (unchecked) prefix end of {@code s}, which is at most a window long. */
  int prefixEndWithin(String s, int limit) {
    if (s.length() > windowChars) {
      throw failure(PackFailures.Kind.INTERNAL, "tokenizer input of " + s.length() + " chars exceeds the window");
    }
    try {
      return tokenizer.prefixEndWithin(s, limit);
    } catch (RuntimeException e) {
      throw failures.failure(PackFailures.Kind.TOKENIZER_ERROR, "tokenizer " + tokenizer.id() + " failed", e);
    }
  }

  /**
   * A large code-point end {@code e} of {@code s} with {@code fits(head + s[0, e))}, found by a binary search whose lower bound
   * only moves to ends it has checked; 0 when not even one code point fits after {@code head}.
   */
  int checkedPrefixEnd(String head, String s) {
    if (fits(head + s)) {
      return s.length();
    }
    int fitsEnd = 0;
    int exceeds = s.length();
    while (true) {
      int mid = (fitsEnd + exceeds) >>> 1;
      if (mid > 0 && mid < s.length() && Character.isLowSurrogate(s.charAt(mid)) && Character.isHighSurrogate(s.charAt(mid - 1))) {
        mid--;
      }
      if (mid <= fitsEnd) {
        mid = s.offsetByCodePoints(fitsEnd, 1);
      }
      if (mid >= exceeds) {
        return fitsEnd;
      }
      if (fits(head + s.substring(0, mid))) {
        fitsEnd = mid;
      } else {
        exceeds = mid;
      }
    }
  }

  /** maximal runs of non-whitespace code points. */
  public static int words(CharSequence s) {
    int count = 0;
    boolean inWord = false;
    for (int i = 0; i < s.length(); ) {
      int cp = Character.codePointAt(s, i);
      boolean whitespace = Character.isWhitespace(cp);
      if (!whitespace && !inWord) {
        count++;
      }
      inWord = !whitespace;
      i += Character.charCount(cp);
    }
    return count;
  }
}
