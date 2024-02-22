from collections import namedtuple

from PyPDF2 import PdfMerger
import datetime
import pandas as pd
import os
import shutil
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from syscore.objects import resolve_function
from syscore.constants import arg_not_supplied
from syscore.fileutils import get_resolved_pathname
from syscore.dateutils import datetime_to_long
from syscore.interactive.display import (
    landing_strip_from_str,
    landing_strip,
    centralise_text,
)
from sysdata.data_blob import dataBlob

from syslogdiag.email_via_db_interface import (
    send_production_mail_msg,
    send_production_mail_msg_attachment,
)

from sysproduction.reporting.report_configs import reportConfig

from pylatex import (
    Document, Section, Subsection, Tabular, Math, TikZ, Axis,
    Plot, Figure, Matrix, Alignat, NoEscape, NewLine, Command,
    PageStyle, Head, Foot, Itemize, LineBreak
)
from pylatex.utils import italic


figure = namedtuple("figure", "pdf_filename")


class ParsedReport(object):
    def __init__(
        self, text: str = arg_not_supplied, pdf_filename: str = arg_not_supplied, csv_table_filenames: list = arg_not_supplied,
    ):

        self._text = text
        self._pdf_filename = pdf_filename
        self._csv_table_filenames = csv_table_filenames

    @property
    def contains_pdf(self) -> bool:
        return self.pdf_filename is not arg_not_supplied

    @property
    def text(self) -> str:
        return self._text

    @property
    def pdf_filename(self) -> str:
        return self._pdf_filename

    @property
    def csv_tables_filenames(self) -> list:
        return self._csv_table_filenames

    @property
    def contains_csv_tables(self) -> bool:
        return self.csv_tables_filenames is not arg_not_supplied and len(self.csv_tables_filenames) > 0


def run_report(report_config: reportConfig, data: dataBlob = arg_not_supplied):
    """

    :param report_config:
    :return:
    """
    pandas_display_for_reports()
    if data is arg_not_supplied:
        data = dataBlob(log_name="Reporting %s" % report_config.title)

    run_report_with_data_blob(report_config, data)


def run_report_with_data_blob(report_config: reportConfig, data: dataBlob):
    """

    :param report_config:
    :return:
    """

    data.log.debug("Running report %s" % str(report_config))

    report_results = run_report_from_config(report_config=report_config, data=data)
    parsed_report = parse_report_results(data=data, report_results=report_results)

    output_report(parsed_report=parsed_report, report_config=report_config, data=data)


def run_report_from_config(report_config: reportConfig, data: dataBlob) -> list:

    report_function = resolve_function(report_config.function)
    report_kwargs = report_config.kwargs

    report_results = report_function(data, **report_kwargs)

    return report_results


def parse_report_results(data: dataBlob, report_results: list) -> ParsedReport:
    """
    Parse report results into human readable text for display, email, or christmas present

    :param report_results: list of header, body or table
    :return: String, with more \n than you can shake a stick at
    """

    if report_contains_figures(report_results):
        output_string = parse_report_results_contains_figures(data, report_results)
    else:
        # output_string = parse_report_results_contains_text(report_results)
        output_string = parse_report_results_contains_text_latex(data, report_results)

    return output_string


def report_contains_figures(report_results: list) -> bool:
    any_figures_in_report = any(
        [type(report_item) is figure for report_item in report_results]
    )

    return any_figures_in_report


def parse_report_results_contains_text(report_results: list) -> ParsedReport:
    """
    Parse report results into human readable text for display, email, or christmas present

    :param report_results: list of header, body or table
    :return: String, with more \n than you can shake a stick at
    """
    output_string = ""
    for report_item in report_results:
        if isinstance(report_item, header):
            parsed_item = parse_header(report_item)
        elif isinstance(report_item, body_text):
            parsed_item = parse_body(report_item)
        elif isinstance(report_item, table):
            parsed_item = parse_table(report_item)
        else:
            parsed_item = " %s failed to parse in report\n" % str(report_item)

        output_string = output_string + parsed_item

    parsed_report = ParsedReport(text=output_string)

    return parsed_report


def parse_report_results_contains_text_latex(data: dataBlob, report_results):
    """
    Parse report results into latex pdf for display, email, or christmas present

    :param report_results: list of header, body or table
    :return: String, with more \n than you can shake a stick at
    """
    geometry_options = {"paper": "a2paper", "margin": "1in"}
    doc = Document(geometry_options=geometry_options)

    output_text = ""
    csv_table_filenames = []
    for report_item in report_results:
        if isinstance(report_item, header):
            if report_item == report_results[0]:
                parsed_item = parse_header_latex(report_item)
            else:
                parsed_item = parse_section_latex(report_item)

        elif isinstance(report_item, body_text):
            parsed_item = parse_body_latex(report_item)
        elif isinstance(report_item, table):
            parsed_item = parse_table_latex(data, report_item)
        else:
            parsed_item = "doc.append(\"%s failed to parse in report\")\ndoc.append(NewLine())\n" % str(report_item)

        if isinstance(parsed_item, tuple):
            csv_table_filenames += parsed_item[0]
            parsed_item = parsed_item[-1]

        output_text = output_text + parsed_item

    # End of report:
    # doc.append(NewLine())
    output_text = "doc.append(Command(\"huge\"))\n\n" + output_text
    output_text = output_text + "doc.append(NoEscape(r\"\\noindent\\rule{\\textwidth}{1pt}\"))\n"
    # output_text = output_text + "doc.change_document_style(\"header\")\n"
    output_text = (output_text + "doc.append(Command(\"centering\"))\ndoc.append(Command(\"Huge\"))\n" +
                   "doc.append(\" \")\ndoc.append(NewLine())\ndoc.append(\"END OF REPORT\")\n")
    print(output_text)
    exec(output_text)
    temp_filename = _generate_temp_pdf_filename(data)
    doc.generate_pdf(temp_filename[:-4], clean_tex=False)

    parsed_report = ParsedReport(pdf_filename=temp_filename, csv_table_filenames=csv_table_filenames)

    return parsed_report


table = namedtuple("table", "Heading Body")


def parse_table(report_table: table) -> str:
    table_header = report_table.Heading
    table_body = str(report_table.Body)
    table_header_centred = centralise_text(table_header, table_body)
    underline_header = landing_strip_from_str(table_header_centred)

    table_string = "\n%s\n%s\n%s\n\n%s\n\n" % (
        underline_header,
        table_header_centred,
        underline_header,
        table_body,
    )

    return table_string


body_text = namedtuple("bodytext", "Text")


def parse_body(report_body: body_text) -> str:
    body_text = report_body.Text
    return "%s\n" % body_text


header = namedtuple("header", "Heading")


def parse_header(report_header: header) -> str:
    header_line = landing_strip(80, "*")
    header_text = centralise_text(report_header.Heading, header_line)

    return "\n%s\n%s\n%s\n\n\n" % (header_line, header_text, header_line)


def parse_section_latex(report_section: header):
    if report_section.Heading.upper() == "END OF REPORT":
        section_string = ""
    else:
        section_string = "doc.append(NewLine())\nwith doc.create(Section(\"%s\")):\n\t" % report_section

    return section_string


def parse_header_latex(report_header: header):
    # header_string = "doc.preamble.append(Command('title', \"%s\"))\n" % report_header
    # header_string += "doc.append(NoEscape(r'\\maketitle'))\n"
    header_string = ("doc.append(Command(\"centering\"))\ndoc.append(Command(\"Huge\"))\n" +
                     "doc.append(\"%s\")\ndoc.append(NewLine())\ndoc.append(NewLine())\n" % report_header +
                     "doc.append(Command(\"raggedright\"))\ndoc.append(Command(\"huge\"))\n")

    return header_string


def parse_body_latex(report_body: body_text):
    report_body = str(report_body.Text).replace("_", r"\_").replace("&", r"\&").replace("$", r"\$").split('\n')
    body_string = ""
    itemize = False
    for report_line in report_body:
        if len(report_line) > 0:
            if report_line[0] == '-':
                if not itemize:
                    body_string += "with doc.create(Itemize()) as itemize:\n"
                    body_string += "\titemize.add_item(\"%s\")\n" % report_line[2:]
                    itemize = True
                else:
                    body_string += "\titemize.add_item(\"%s\")\n" % report_line[2:]
            else:
                itemize = False
                body_string += "\ndoc.append(NoEscape(\"%s \"))\n" % report_line
                if report_line == report_body[-1]:
                    body_string += "doc.append(NewLine())\n"
        # else:
        #     body_string += "doc.append(NewLine())\n"

    # body_string = "doc.append(\"%s. \")\ndoc.append(NewLine())\n" * len(report_body)
    # body_string = body_string % tuple(report_body)

    return body_string


def parse_table_latex(data: dataBlob, report_table: table):
    table_header = report_table.Heading
    # table_string = ("\ndoc.append(Command(\"centering\"))\ndoc.append(Command(\"Huge\"))\n" +
    #                 "doc.append(\"%s\")\ndoc.append(NewLine())\ndoc.append(NewLine())\n" % table_header +
    #                 "doc.append(Command(\"raggedright\"))\ndoc.append(Command(\"huge\"))\n")
    table_string = "doc.append(\"%s\"); doc.append(NewLine())\n" % table_header
    table_body = report_table.Body
    table_body.dropna(axis=0, how='any', inplace=True)
    if table_body.index.is_numeric():
        # table_body.reset_index(inplace=True, drop=True)
        table_body.index = [str(i) for i in table_body.index]

    temp_filename = _generate_temp_pdf_filename(data)[:-4] + '_' + table_header.replace('_', ' ').replace('.', '-').replace('/', '') + ".csv"
    table_body.to_csv(temp_filename)

    '''
    table_body = report_table.Body
    table_body.dropna(axis=0, how='any', inplace=True)
    if table_body.index.is_numeric():
        # table_body.reset_index(inplace=True, drop=True)
        table_body.index = [str(i) for i in table_body.index]

    if len(table_body) == 0:
        return "doc.append(\" - Empty DataFrame - \")\n"

    print("\n\n\n\n%s\n\n\n\n" % table_body)

    if table_body.ndim > 1:
        table_cols_before = table_body.shape[1] + 1
        table_body.rename(columns=dict(zip(table_body.columns, [str(col).replace(' ', '_') for col in table_body.columns])), inplace=True)
        for j, col in table_body.iteritems():
            table_body[j] = pd.Series([str(table_body[j].iloc[i]).replace(' ', '_') for i in range(len(col))])

        table_body = table_body.to_string(sparsify=False).lstrip()
        table_cols = len(table_body.split('\n')[-1].split())
    else:
        table_cols_before = 1
        table_body = pd.Series([str(table_body.iloc[i]).replace(' ', '_') for i in range(len(table_body))])
        table_body = table_body.to_string().lstrip()
        table_cols = table_cols_before + 1

    # table_cols = table_body.shape[1] + 1
    # table_rows = table_body.shape[0]

    print(table_body)

    table_header = (
        "with doc.create(Subsection(\"%s\")):\n\t\tdoc.append(Command(\"centering\"))\n\t\tdoc.append(Command(\"scriptsize\"))\n\t\twith doc.create(Tabular(\"%s\")) as table:\n\t\t\t" % (
            report_table.Heading.replace("_", " ").replace("&", r"\&").replace("$", r"\$"), '|c' * table_cols + '|'
        )
    )

    table_string = table_body.split('\n')
    for i in range(len(table_string)):
        if i == 0:
            table_string_i_split = table_string[i].split()
            empty_spaces = table_cols - len(table_string_i_split)
            if empty_spaces < 0:
                empty_spaces = 0

            table_string[i] = (" \", \"" * empty_spaces + "\", \"".join(table_string_i_split))

        else:
            table_string[i] = "\", \"".join(table_string[i].split())

        if i < len(table_string) - 1:
            table_string[i] = table_string[i] + "\"))\n\t\t\ttable.add_row((\""

    table_string = ''.join(table_string)
    table_string = (table_header +
                    "table.add_hline()\n\t\t\ttable.add_row((\"" +
                    table_string +
                    "\"))\n\t\t\ttable.add_hline()\n" +
                    "\ndoc.append(Command(\"centerline\", arguments=NewLine()))\n" + # arguments=NoEscape(\"\\rule{500pt, 1pt}\"))
                    "\ndoc.append(Command(\"huge\"))\ndoc.append(Command(\"raggedright\"))\n")

    return table_string
    '''

    return [temp_filename], table_string


def parse_report_results_contains_figures(
    data: dataBlob, report_results: list
) -> ParsedReport:
    merger = PdfMerger()

    for report_item in report_results:
        if type(report_item) is not figure:
            data.log.critical("Reports can be all figures or all text for now")
            raise Exception()
        pdf = report_item.pdf_filename
        merger.append(pdf)

    merged_filename = _generate_temp_pdf_filename(data)

    merger.write(merged_filename)
    merger.close()

    parsed_report = ParsedReport(pdf_filename=merged_filename)

    return parsed_report


def pandas_display_for_reports():
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_columns", 1000)
    pd.set_option("display.max_rows", 1000)


def output_report(
    data: dataBlob, report_config: reportConfig, parsed_report: ParsedReport
):

    output = report_config.output

    # We either print or email or send to file or ...
    if output == "console":
        display_report(parsed_report)
    elif output == "email":
        email_report(parsed_report, report_config=report_config, data=data)
    elif output == "file":
        output_file_report(parsed_report, report_config=report_config, data=data)
    elif output == "emailfile":
        email_report(parsed_report, report_config=report_config, data=data)
        output_file_report(parsed_report, report_config=report_config, data=data)
    else:
        raise Exception("Report config output destination %s not recognised!" % output)


def display_report(parsed_report: ParsedReport):
    ### What if pdf?
    if parsed_report.contains_pdf:
        display_pdf_report(parsed_report)
    else:
        print(parsed_report.text)


def display_pdf_report(parsed_report: ParsedReport):
    pdf_filename = parsed_report.pdf_filename
    print("Trying to display %s" % pdf_filename)
    try:
        ## thing
        os.system("evince %s" % pdf_filename)
    except:
        print(
            "Display pdf with evince doesn't seem to work with your OS or perhaps headless terminal?"
        )


def email_report(
    parsed_report: ParsedReport, report_config: reportConfig, data: dataBlob
):

    if parsed_report.contains_pdf or parsed_report.contains_csv_tables:
        filename = []
        if parsed_report.contains_pdf:
            filename += [parsed_report.pdf_filename]
        if parsed_report.contains_csv_tables:
            filename += parsed_report.csv_tables_filenames

        send_production_mail_msg_attachment(
            body="Report attached",
            subject=report_config.title,
            filename=filename,
        )
    else:
        send_production_mail_msg(
            data=data,
            body=parsed_report.text,
            subject=report_config.title,
            email_is_report=True,
        )


def output_file_report(
    parsed_report: ParsedReport, report_config: reportConfig, data: dataBlob
):
    full_filename = resolve_report_filename(report_config=report_config, data=data)
    if parsed_report.contains_pdf:
        ## Already a file so just rename temp file name to final one
        pdf_full_filename = "%s.pdf" % full_filename
        shutil.copyfile(parsed_report.pdf_filename, pdf_full_filename)
    else:
        write_text_report_to_file(
            report_text=parsed_report.text, full_filename=full_filename
        )

    data.log.debug("Written report to %s" % full_filename)


def resolve_report_filename(report_config, data: dataBlob):
    filename_with_spaces = report_config.title
    filename = filename_with_spaces.replace(" ", "_")
    use_directory = get_directory_for_reporting(data)
    use_directory_resolved = get_resolved_pathname(use_directory)
    full_filename = os.path.join(use_directory_resolved, filename)

    return full_filename


def get_directory_for_reporting(data):
    # eg '/home/rob/reports/'
    production_config = data.config
    store_directory = production_config.get_element("reporting_directory")
    return store_directory


def write_text_report_to_file(report_text: str, full_filename: str):
    with open(full_filename, "w") as f:
        f.write(report_text)


class PdfOutputWithTempFileName:
    """
    # generate some kind of plot, then call:
    pdf_output = PdfOutputWithTempFileName(data)
    figure_object = pdf_output.save_chart_close_and_return_figure()

    """

    def __init__(self, data: dataBlob, reporting_directory=arg_not_supplied):
        self._temp_file_name = _generate_temp_pdf_filename(
            data, reporting_directory=reporting_directory
        )

    def save_chart_close_and_return_figure(self) -> figure:
        with PdfPages(self.temp_file_name) as export_pdf:
            export_pdf.savefig()

        plt.close()
        return figure(pdf_filename=self.temp_file_name)

    @property
    def temp_file_name(self) -> str:
        return self._temp_file_name


TEMPFILE_PATTERN = "_tempfile"


def _generate_temp_pdf_filename(
    data: dataBlob, reporting_directory=arg_not_supplied
) -> str:
    if reporting_directory is arg_not_supplied:
        use_directory = get_directory_for_reporting(data)
    else:
        use_directory = reporting_directory

    use_directory_resolved = get_resolved_pathname(use_directory)
    filename = "%s_%s.pdf" % (
        TEMPFILE_PATTERN,
        str(datetime_to_long(datetime.datetime.now())),
    )
    full_filename = os.path.join(use_directory_resolved, filename)

    return full_filename

