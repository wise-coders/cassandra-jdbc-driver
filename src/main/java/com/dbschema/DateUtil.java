package com.dbschema;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * Free to use, code improvements allowed only to the repository https://github.com/wise-coders/cassandra-jdbc-driver
 * Please create pull requests or issues.
 */

class DateUtil {
    private static final SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat utcTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    private static final SimpleDateFormat utcDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    static {
        utcDateFormat.setTimeZone(UTC);
        utcTimeFormat.setTimeZone(UTC);
        utcDateTimeFormat.setTimeZone(UTC);
    }

    private static SimpleDateFormat getDateFormat(TimeZone timeZone) {
        if (timeZone.equals(UTC)) return utcDateFormat;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(timeZone);
        return dateFormat;
    }

    private static SimpleDateFormat getTimeFormat(TimeZone timeZone) {
        if (timeZone.equals(UTC)) return utcTimeFormat;
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        timeFormat.setTimeZone(timeZone);
        return timeFormat;
    }

    private static SimpleDateFormat getDateTimeFormat(TimeZone timeZone) {
        if (timeZone.equals(UTC)) return utcDateTimeFormat;
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        dateTimeFormat.setTimeZone(timeZone);
        return dateTimeFormat;
    }

    static Timestamp considerTimeZone(Timestamp timestamp, Calendar calendar, Direction direction) throws SQLException {
        long time = considerTimeZone(timestamp, calendar.getTimeZone(), direction, DateUtil::getDateTimeFormat);
        Timestamp result = new Timestamp(time);
        result.setNanos(timestamp.getNanos());
        return result;
    }

    static java.sql.Date considerTimeZone(java.sql.Date timestamp, Calendar calendar, Direction direction) throws SQLException {
        long time = considerTimeZone(timestamp, calendar.getTimeZone(), direction, DateUtil::getDateFormat);
        return new java.sql.Date(time);
    }

    static Time considerTimeZone(Time timestamp, Calendar calendar, Direction direction) throws SQLException {
        long time = considerTimeZone(timestamp, calendar.getTimeZone(), direction, DateUtil::getTimeFormat);
        return new Time(time);
    }

    private static <T extends java.util.Date> long considerTimeZone(T date, TimeZone timeZone, Direction direction,
                                                                    Function<TimeZone, DateFormat> formatSupplier) throws SQLException {
        String startValue = direction.formatter(timeZone, formatSupplier).format(date);
        try {
            Date resultDate = direction.parser(timeZone, formatSupplier).parse(startValue);
            return resultDate.getTime();
        } catch (ParseException e) {
            throw new SQLException(e);
        }
    }

    enum Direction {
        FROM_UTC {
            @Override
            DateFormat formatter(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier) {
                return formatSupplier.apply(UTC);
            }

            @Override
            DateFormat parser(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier) {
                return formatSupplier.apply(timeZone);
            }
        },
        TO_UTC {
            @Override
            DateFormat formatter(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier) {
                return FROM_UTC.parser(timeZone, formatSupplier);
            }

            @Override
            DateFormat parser(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier) {
                return FROM_UTC.formatter(timeZone, formatSupplier);
            }
        };

        abstract DateFormat formatter(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier);

        abstract DateFormat parser(TimeZone timeZone, Function<TimeZone, DateFormat> formatSupplier);
    }
}
