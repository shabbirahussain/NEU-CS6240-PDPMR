package org.neu.pdpmr.tasks.types;

import org.neu.pdpmr.tasks.util.DateTimeUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author shabbira.hussain
 */
public class Query implements Comparable<Query>{
    private static final SimpleDateFormat df = new SimpleDateFormat("(YYYY, MM, dd)");
    public long dateMs;
    public String orig, dest;

    public Query(final String orig,
                 final String dest,
                 final long dateMs){
        this.orig = orig;
        this.dest = dest;
        this.dateMs = dateMs;
    }

    public Query(final String queryStr){
        String queries[] = queryStr.split(",");
        this.orig = queries[3].trim().toUpperCase();
        this.dest = queries[4].trim().toUpperCase();
        int year    = Integer.parseInt(queries[0]);
        int month   = Integer.parseInt(queries[1]);
        int day     = Integer.parseInt(queries[2]);
        this.dateMs = DateTimeUtil.getTimeInMs(year, month, day, 0);
    }

    @Override
    public int compareTo(Query o) {
        int res;
        if((res = orig.compareTo(o.orig)) != 0) return res;
        if((res = dest.compareTo(o.dest)) != 0) return res;
        return (int)(dateMs - o.dateMs);
    }

    @Override
    public String toString(){
        return df.format(new Date(this.dateMs)) + ":" + this.orig + "->" + this.dest;
    }
}
