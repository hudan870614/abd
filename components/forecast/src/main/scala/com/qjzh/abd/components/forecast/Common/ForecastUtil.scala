package com.qjzh.abd.components.forecast.Common

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by damon on 2016/12/20.
  */
object ForecastUtil
{
    def getWeek(dateString:String,formatString:String="yyyyMMdd"):Int=
    {
      val sdfInput = new SimpleDateFormat( formatString );
      val calendar = Calendar.getInstance();
      val date = sdfInput.parse(dateString)

      calendar.setTime(date);
      var dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
      dayOfWeek
    }
}
