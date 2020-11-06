using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csvtosql
{
    [Serializable()]
    class CustomException : System.Exception
    {         
            public CustomException() : base() { }
            public CustomException(string message) : base(message) { }
            public CustomException(string message, System.Exception inner) : base(message, inner) { }

            // A constructor is needed for serialization when an
            // exception propagates from a remoting server to the client. 
            protected CustomException(System.Runtime.Serialization.SerializationInfo info,
                System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
    enum ExitCode
    {
        None = 0,       // Success, No Errors
        Warning = 1,    // Warning
        Error = 2       // Fatal Error

    }

    enum ActionOnError
    {
        Continue = 0,           // Continue, no errors, no logging
        LogWarning = 1,         // Log warning and continue
        ExitWithFailure = 2,    // Log fatal error and exit app
        Ignore = 3,             // No Action, ignore errors and warnings, no logging
        Debug = 4               // Debug - Log errors and warnings, do not exit
    }
    enum ErrorLevel
    {
        None = 0,           // No errors/warnings allowed; Exit if any E/W
        Warning = 1,        // Warnings allowed; if Error - exit
        Error = 2         // Errors allowed; continue untill get all warning/errors to the end of app
    }
    enum MetadataSource
    {
        Database = 0,           // Database
        TextFile = 1,        // Text file .ini
        JSONFile = 2,         // JSON File
        XmlFile = 4         //XML FIle
    }
    enum ValidationOptions
    {
        ValidateBasic = 1,     /// <summary>
                               ///  Business rules defined in metadata, as well as File TableSchema
                               /// </summary>
        ValidateAsTable = 2, /// <summary>
                             /// Rules based on existing SQL table - FK, Checks, Constraints, Unique Keys
                             /// </summary>
        //ValidateExtended = 4 , 
        LoadToFile = 4, /// <summary>
                        /// Output to correctly formatted file, with correct field order, may also add fields (future)
                        /// </summary>
        LoadToTableBCP = 8 /// Load the file to Sql Table using BCP
    }
    public class SumWarning
    {
        public string FieldName;
        public string RuleDefinition;
        public bool IsError;
        public long Cnt;
        public SumWarning() { }
        public SumWarning(string fieldname, string rule, long cnt , bool is_error) 
        {
            FieldName = fieldname;
            RuleDefinition = rule;
            Cnt = cnt;
            IsError = is_error;
        }
    }
    public class FormatWarning
    {
        public string GetPkValue(int ix)
        {
            if ((PkValues.Count == 0) || (PkValues == null) || (PkValues.Count <= ix))
                return string.Empty;
            else // ((PkValues.Count == PkFields.Count))
                return PkValues[ix].ToString();
        }
        public void SetPkFields(List<string> pkfields)
        {
            PkFields = pkfields;
            PkValues = pkfields.Select(v => "''").ToList(); 
        }
        public void SetPkValues(List<string> pkfields)
        {
            PkFields = pkfields;
        }
        public List<string> GetPkValues()
        {
            return PkFields;
        }
        public void SetValues(string column_name, string value, string fail_reson, long line_num)
        {
            FieldName = column_name;
            FieldValue = value;
            Message = fail_reson;
            LineNumber = line_num;
        }
        public void SetValues(List<string> pkvalues, string column_name, string value, string fail_reson, long line_num)
        {
            PkValues = new List<string>();
            pkvalues.ForEach(p => PkValues.Add(p.ToString()));
            FieldName = column_name;
            FieldValue = value;
            Message = fail_reson;
            LineNumber = line_num;
        }
        public void SetValues(string column_name, string value, string fail_reson, long line_num, string rule_def) {
            FieldName = column_name;
            FieldValue = value;
            Message = fail_reson;
            LineNumber = line_num;
            RuleDefinition = rule_def;
        }
        public void SetValues(List<string> pkvalues, string column_name, string value, string fail_reson, long line_num, string rule_def)
        {
            PkValues = new List<string>();
            pkvalues.ForEach(p => PkValues.Add(p.ToString()));
            FieldName = column_name;
            FieldValue = value;
            Message = fail_reson;
            LineNumber = line_num;
            RuleDefinition = rule_def;
        }
    public string FieldName;
        public string FieldValue;
        public string Message;
        public long LineNumber = -1;
        public DateTime Date = DateTime.Now;
        public string RuleDefinition;
        public List<string> GetAllValues() 
        {
            List<string> allvals = PkValues.ToList();
            if ((PkValues != null) && (PkValues.Count > 0))
            {
                allvals = PkValues.Select(x => !x.StartsWith("'") && !x.EndsWith("'") ? x.Quotename("'") : x).ToList();
            }
            else
            if ((PkFields != null) && (PkFields.Count > 0))
                allvals = PkFields.Select(c => "''").ToList();

            allvals.AddRange(WarnValues().Select(x => !x.StartsWith("'") && !x.EndsWith("'") ? x.Quotename("'") : x));
           
            return allvals;
        }
        public List<string> PkValues;
        public List<string> PkFields;
        public List<string> WarnValues()
        {

            return new List<string>() { FieldName ?? string.Empty
                , FieldValue != null ?  (FieldValue.Length > 4000 ? FieldValue.Substring(0, 4000) : FieldValue): string.Empty
                , Message != null ? (Message.Length > 4000 ? Message.Substring(0, 4000) : Message) : string.Empty
                , LineNumber.ToString() };
        }  
        public List<string> WarningValues()
        {
            return new List<string>() { FieldName, FieldValue, Message, LineNumber.ToString(), Date.ToLongDateString() };
        }
        public List<string> WarningValues(string column_name, string value, string fail_reson, long line_num)
        {
            return new List<string>() { column_name, value, Message, fail_reson, line_num.ToString(), DateTime.Now.ToLongDateString() };
        }
        public List<string> WarningValues(List<string> pkvalues, string column_name, string value, string fail_reson, long line_num)
        {
            pkvalues.Add(column_name);
            pkvalues.Add(value);
            pkvalues.Add(fail_reson);
            pkvalues.Add(line_num.ToString());
            pkvalues.Add(DateTime.Now.ToLongDateString());

            return pkvalues;
        }
        public string WarningValuesToString(List<string> pkvalues, string column_name, string value, string fail_reson, long line_num)
        {
            pkvalues.Add(column_name);
            pkvalues.Add(value);
            pkvalues.Add(fail_reson);
            pkvalues.Add(line_num.ToString());
            pkvalues.Add(DateTime.Now.ToLongDateString());

            return string.Join(",",pkvalues);
        }
    }
}
