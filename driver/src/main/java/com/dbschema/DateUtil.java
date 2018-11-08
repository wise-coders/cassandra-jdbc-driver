package com.dbschema;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @author Liudmila Kornilova
 **/
class DateUtil {
    static final SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    static final SimpleDateFormat utcTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    static final SimpleDateFormat utcDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        utcTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        utcDateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    static SimpleDateFormat getDateFormat(TimeZone timeZone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(timeZone);
        return dateFormat;
    }

    static SimpleDateFormat getTimeFormat(TimeZone timeZone) {
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        timeFormat.setTimeZone(timeZone);
        return timeFormat;
    }

    static SimpleDateFormat getDateTimeFormat(TimeZone timeZone) {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        dateTimeFormat.setTimeZone(timeZone);
        return dateTimeFormat;
    }
}
