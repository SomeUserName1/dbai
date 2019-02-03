/*
 * @(#)DateUtils.java   1.0   May 17, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

import java.util.Calendar;
import java.util.Date;

/**
 * Utils for dates.
 *
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 * @version 1.0
 */
public final class DateUtils {

   /**
    * private.
    */
   private DateUtils() {
   }

   /**
    * Adds days to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addDays(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.DAY_OF_YEAR, amount);
      return calendar.getTime();
   }

   /**
    * Adds weeks to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addWeeks(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.WEEK_OF_YEAR, amount);
      return calendar.getTime();
   }

   /**
    * Adds months to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addMonths(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.MONTH, amount);
      return calendar.getTime();
   }

   /**
    * Adds years to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addYears(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.YEAR, amount);
      return calendar.getTime();
   }

   /**
    * Adds hours to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addHours(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.HOUR_OF_DAY, amount);
      return calendar.getTime();
   }

   /**
    * Adds minutes to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addMinutes(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.MINUTE, amount);
      return calendar.getTime();
   }

   /**
    * Adds seconds to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addSeconds(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.SECOND, amount);
      return calendar.getTime();
   }

   /**
    * Adds milliseconds to a date.
    * 
    * @param date
    *           the date
    * @param amount
    *           the time delta
    * @return the new date
    */
   public static Date addMilliseconds(final Date date, final int amount) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.MILLISECOND, amount);
      return calendar.getTime();
   }

   /**
    * Adds a date interval to a date.
    * 
    * @param date
    *           the date
    * @param years
    *           delta years
    * @param months
    *           delta months
    * @param days
    *           delta days
    * @return the new date
    */
   public static Date addDate(final Date date, final int years, final int months, final int days) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.YEAR, years);
      calendar.add(Calendar.MONTH, months);
      calendar.add(Calendar.DAY_OF_YEAR, days);
      return calendar.getTime();
   }

   /**
    * Adds a time interval to a date.
    * 
    * @param date
    *           the date
    * @param hours
    *           delta hours
    * @param minutes
    *           delta minutes
    * @param seconds
    *           delta seconds
    * @return the new date
    */
   public static Date addTime(final Date date, final int hours, final int minutes, final int seconds) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.HOUR_OF_DAY, hours);
      calendar.add(Calendar.MINUTE, minutes);
      calendar.add(Calendar.SECOND, seconds);
      return calendar.getTime();
   }

   /**
    * Adds an interval to a date.
    * 
    * @param date
    *           the date
    * @param years
    *           delta years
    * @param months
    *           delta months
    * @param days
    *           delta days
    * @param hours
    *           delta hours
    * @param minutes
    *           delta minutes
    * @param seconds
    *           delta seconds
    * @return the new date
    */
   public static Date add(final Date date, final int years, final int months, final int days, final int hours,
         final int minutes, final int seconds) {
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      calendar.add(Calendar.YEAR, years);
      calendar.add(Calendar.MONTH, months);
      calendar.add(Calendar.DAY_OF_YEAR, days);
      calendar.add(Calendar.HOUR_OF_DAY, hours);
      calendar.add(Calendar.MINUTE, minutes);
      calendar.add(Calendar.SECOND, seconds);
      return calendar.getTime();
   }
}
