/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;

public class DateMacroTest {

    private final DateMacro dateMacro = new DateMacro();

    private static Locale defaultLocale;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setLocale() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.UK);
    }

    @AfterClass
    public static void resetLocale() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void applyDateMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        dateMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyDateMacroToOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        dateMacro.apply(ImmutableList.of("10/05/2017"));
    }

    @Test
    public void applyDateMacroToMoreThanTwoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        dateMacro.apply(ImmutableList.of("1", "2", "3"));
    }

    @Test
    public void applyDateMacroToInvalidFormat_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot parse date format");

        dateMacro.apply(ImmutableList.of("10/05/2017", "invalid"));
    }

    @Test
    public void applyDateMacroToDateNotParseableByFormat_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot parse date value");

        dateMacro.apply(ImmutableList.of("invalid", "MM/dd/yyyy"));
    }

    @Test
    public void applyDateMacroToDate_ReturnsISODateTime(){
        String date = "10/09/1993";
        String format = "MM/dd/yyyy";

        Unescaped<String> result = dateMacro.apply(ImmutableList.of(date, format));

        String formattedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(format)).atStartOfDay().format(DateTimeFormatter.ISO_DATE_TIME);
        assertEquals(formattedDate, result.toString());
    }

    @Test
    public void applyDateMacroToTime_ReturnsISODateTime(){
        String date = "10:15";
        String format = "H:m";

        Unescaped<String> result = dateMacro.apply(ImmutableList.of(date, format));

        String formattedDate = LocalTime.parse(date, DateTimeFormatter.ofPattern(format)).atDate(LocalDate.now()).format(DateTimeFormatter.ISO_DATE_TIME);
        assertEquals(formattedDate, result.toString());
    }

    @Test
    public void applyDateMacroToDateTime_ReturnsISODateTime(){
        String date = "June 30 2017 10:15";
        String format = "MMMM dd yyyy H:m";

        Unescaped<String> result = dateMacro.apply(ImmutableList.of(date, format));

        String formattedDate = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(format)).format(DateTimeFormatter.ISO_DATE_TIME);
        assertEquals(formattedDate, result.toString());

    }

    @Test
    public void whenDateMacroCalledWithMoreThanTwoArguments_ExceptionIsThrown(){
        String template = "insert $x val @date(<date>, \"mm/dd/yyyy\", \"dd/mm/yyyy\");";

        exception.expect(GraqlQueryException.class);

        Graql.parser().parseTemplate(template, Collections.singletonMap("date", "10/09/1993"));
    }

    // Templating tests
    @Test
    public void whenDateMacroCalledWithDateFormat_DateFormatConvertedToISODateTime() throws Exception {
        String dateTimePattern = "MM/dd/yyyy";
        String dateAsString = "10/09/1993";

        String formattedTime = LocalDate
                .parse(dateAsString, DateTimeFormatter.ofPattern(dateTimePattern)).atStartOfDay()
                .format(DateTimeFormatter.ISO_DATE_TIME);

        String template = "insert $x val @date(<date>, \"" + dateTimePattern + "\");";
        String expected = "insert $x0 val " + formattedTime + ";";

        assertParseEquals(template, Collections.singletonMap("date", dateAsString), expected);
    }

    @Test
    public void whenDateMacroCalledWithDateTimeFormat_DateFormatConvertedToISODateTime() throws Exception {
        String dateTimePattern = "yyyy-MM-dd HH:mm";
        String dateAsString = "1986-04-08 12:30";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimePattern);

        String formattedTime = LocalDateTime
                .parse(dateAsString, formatter)
                .format(DateTimeFormatter.ISO_DATE_TIME);

        String template = "insert $x val @date(<date>, \"" + dateTimePattern + "\");";
        String expected = "insert $x0 val " + formattedTime + ";";

        assertParseEquals(template, Collections.singletonMap("date", dateAsString), expected);
    }

    @Test
    public void whenDateMacroCalledAndDateCannotBeParsed_ExceptionIsThrown(){
        String dateTimePattern = "dd-MMM-yyyy HH:mm:ss";
        String dateAsString = "03-feb-2014 01:16:31";

        String template = "insert $x val @date(<date>, \"" + dateTimePattern + "\");";

        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot parse date value");

        assertParseEquals(template, Collections.singletonMap("date", dateAsString), null);
    }

    @Test
    public void whenDateMacroCalledWithInvalidDateFormat_ExceptionIsThrown(){
        String template = "insert $x val @date(<date>, \"this is not a format\");";

        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Cannot parse date format");

        assertParseEquals(template, Collections.singletonMap("date", "10/09/1993"), null);
    }
}
