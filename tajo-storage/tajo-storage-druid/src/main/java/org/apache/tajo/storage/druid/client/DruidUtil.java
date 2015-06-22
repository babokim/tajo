/**
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

package org.apache.tajo.storage.druid.client;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DruidUtil {
  /** Transform Calendar to ISO 8601 string. */
  public static String fromCalendar(final Calendar calendar) {
    Date date = calendar.getTime();
    return fromTimestamp(date.getTime(), true);
  }

  /** Get current date and time formatted as ISO 8601 string. */
  public static String now() {
    return fromCalendar(GregorianCalendar.getInstance());
  }

  /** Transform ISO 8601 string to Calendar. */
  public static Calendar toCalendar(final String iso8601string)
      throws ParseException {
    Calendar calendar = GregorianCalendar.getInstance();
    String s = iso8601string.replace("Z", "+00:00");
    try {
      s = s.substring(0, 26) + s.substring(27);  // to get rid of the ":"
    } catch (IndexOutOfBoundsException e) {
      throw new ParseException("Invalid length", 0);
    }
    Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parse(s);
    calendar.setTime(date);
    return calendar;
  }

  public static String fromTimestamp(long timestamp, boolean includeTimeZone) {
    String formatStr = "yyyy-MM-dd'T'HH:mm:ss";
    if (includeTimeZone) {
      formatStr += "Z";
    }
    String formatted = new SimpleDateFormat(formatStr)
        .format(timestamp);
    if (includeTimeZone) {
      return formatted.substring(0, 22) + ":" + formatted.substring(22);
    } else {
      return formatted;
    }
  }
}
