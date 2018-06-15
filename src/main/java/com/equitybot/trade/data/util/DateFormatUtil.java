package com.equitybot.trade.data.util;

import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;


public class DateFormatUtil {

	public static final String KITE_SERVICE_FORMAT = "E MMM dd HH:mm:ss Z yyyy";
	public static final String KITE_TICK_TIMESTAMP_FORMAT = "MMM dd, yyyy hh:mm:ss a";

	public static final String MONGODB_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
	public static final String TA4J = "yyyy-MM-dd";

	public static String toISO8601UTC(Date date, String dateFormat) {

		Format formatter = new SimpleDateFormat(MONGODB_DATE_FORMAT);
		return formatter.format(date);
	}

	public static Date fromISO8601UTC(String dateStr, String dateFormat) {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat(dateFormat);
		df.setTimeZone(tz);

		try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static ZonedDateTime convertKiteTickTimestampFormat(String kiteTimestamp) throws ParseException {
		return ZonedDateTime.ofInstant(new SimpleDateFormat(KITE_TICK_TIMESTAMP_FORMAT, Locale.US).parse(kiteTimestamp).toInstant(), ZoneId.systemDefault());
	}
	
}

