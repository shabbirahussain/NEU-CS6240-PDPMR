package org.neu.pdpmr.tasks.util;

import java.util.Calendar;

/**
 * @author shabbira.hussain
 */
public abstract class DateTimeUtil {
    /**
     * @param year is the year.
     * @param month 1 index based month.
     * @param day the day of the month.
     * @param hhmm is the hours and minutes of time.
     * @return Returns millisecond time constructed form given arguments.
     */
    public static long getTimeInMs(final int year,
                             final int month,
                             final int day,
                             final int hhmm){
        Calendar cal = Calendar.getInstance();
        cal.set(year, month-1, day, hhmm/100, hhmm%100,0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }
   
}
