/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.template.macro;

import ai.grakn.graql.macro.Macro;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * Parse the given value (arg1) using the format (arg2) into another format (arg3).
 * If no second format (arg3) is provided, converts the given value (arg1) into epoch time.
 * Returns a String with the value.
 *
 * Usage:
 *      {@literal @}date("01/30/2017", "mm/dd/yyyy", "dd/mm/yyyy")
 *      {@literal @}date("01/30/2017", "mm/dd/yyyy")
 * </p>
 *
 * @author alexandraorth
 */
public class DateMacro implements Macro<String> {

    @Override
    public String apply(List<Object> values) {
        if(values.size() != 2 && values.size() != 3){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        String originalDate = values.get(0).toString();
        String originalFormat = values.get(1).toString();
        String newFormat = values.size() == 3 ? values.get(2).toString() : null;

        return convertDateFormat(originalDate, originalFormat, newFormat);
    }

    @Override
    public String name() {
        return "date";
    }

    private String convertDateFormat(String originalDate, String originalFormat, String newFormat){
        originalFormat = removeQuotes(originalFormat);

        SimpleDateFormat originalDateFormat = new SimpleDateFormat(originalFormat);

        try {
            Date date = originalDateFormat.parse(originalDate);

            if (newFormat == null) {
                return Long.toString(date.getTime());
            } else {
                SimpleDateFormat newDateFormat = new SimpleDateFormat(removeQuotes(newFormat));
                return newDateFormat.format(date);
            }
        } catch (ParseException e){
            throw new IllegalArgumentException("Could not parse date " + originalDate + " using " + originalDateFormat.toPattern());
        }
    }

    private String removeQuotes(String str){
        return str.replace("\"", "");
    }
}
