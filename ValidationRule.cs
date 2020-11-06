using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace csvtosql
{
    public static class RxUtils
    {
        public static string strRuleJoin = @"\s*(?<rule>([^&|]+))\s*(?<join>([&|]?)+)(\s?)*";
        public static string strClassName = @"^(((?<ns>(\w+))[.])?((?<cls>(\w+))[.]))?(?<mem>(\w[^\s]+))$";
        public static string[] ParseClassMem(this string input)
        {
            if (!new Regex(strClassName).IsMatch(input))
                return new string[0];
            string[] clsarr = new string[3] { "", "", "" };
            Match mm = new Regex(strClassName).Match(input);
            string ns = mm.Groups["ns"].Value.ToString();
            string cls = mm.Groups["cls"].Value.ToString();
            string mem = mm.Groups["mem"].Value.ToString();
            clsarr[0] = ns == string.Empty ? "csvtosql" : ns;
            clsarr[1] = cls == string.Empty ? "ExtUtils" : cls;
            clsarr[2] = mem;

            return clsarr;
        }

        public static string strValueFunction = @"^\s*[@]\s*[{]\s*(?<func>(\w+))\s*[(]\s*[)]\s*[}]$";
        public static string strParamFunction = @"\s*[@]\s*[{]\s*(?<func>(\w+))\s*[(](?<params>([\w\d]+.+))[)]\s*[}]";
        public static string strParameter = @"\s*\@\s*\{\s*(?<prop>(([\w\d\S]+[^()])))\s*\}\s*";
        public static string strProperty = @"\s*\@\s*\{\s*(?<prop>(([\w\d\S]+[^()])))\s*\}\s*";
        public static string strField = @"\s*([[]?(?<field>(\w[^(]+))[\]]?)\s*";
        public static string strNumeric = @"\s*(\d*.?\d*)|(\d*)\s*";
        public static string strInteger = @"\s*[^\D]+\s*";
        public static string strVRuleElements = @"\s*(?<lval>([^!<>=]+[\s]))\s*(?<comp>([!<>=]+))\s*(?<rval>([^!<>=]+))\s*";
        public static bool IsNumeric(this string input) { return new Regex(strNumeric).IsMatch(input); }
        public static bool IsInteger(this string input) { return new Regex(strInteger).IsMatch(input); }

        public static bool IsValueFunction(this string input) { return new Regex(strValueFunction).IsMatch(input);  }
        public static bool IsParamFunction(this string input) { return new Regex(strParamFunction).IsMatch(input); }
        public static bool IsProperty(this string input) { return new Regex(strProperty).IsMatch(input); }
        //public static bool IsField(this string input) { return new Regex(strField).IsMatch(input); }
        public static bool IsField(this string input, DataColumn[] datafields) 
        {
            return datafields.OfType<DataColumn>().Where(c => c.ColumnName.ToLower() == input.TrimStart('[').TrimEnd(']').ToLower()).ToList().Count() > 0;
                //(new Regex(strField).IsMatch(input)) && (datafields.Select(c => c.ColumnName.ToLower()).Contains(input.ToLower()));               
        }
        public static MatchCollection ToListOfRuleJoins(this string input) { return new Regex(strRuleJoin).Matches(input); }
        public static MatchCollection ToListOfElements(this string input) { return new Regex(strVRuleElements).Matches(input); }
        public static Match ParseAsListOfElements(this string input) { return new Regex(strVRuleElements).Match(input); } 
        public static Match ParseAsValueFunction(this string input) { return new Regex(strValueFunction).Match(input); }
        public static Match ParseAsParamFunction(this string input) { return new Regex(strParamFunction).Match(input); }
        public static Match ParseAsParameter(this string input) { return new Regex(strParameter).Match(input); }
        public static Match ParseAsProperty(this string input) { return new Regex(strProperty).Match(input); }
        public static Match ParseAsField(this string input) { return new Regex(strField).Match(input); }
    }

    //[Serializable]
    class ConstraintEntity
    {
        /// <summary>
        /// Constaint is a list of single rules joined by && or || logical operators.
        /// Each single rule has 2 Elements (LeftElement, RightElement) and CompOperator (><=!)
        /// Each element may be of type Value, Field, Property, ValueFunction, or ParamFunction
        /// Value is numeric or string or date value
        /// Field is representaion of the DataField from which the value would be retrieved at runtime
        /// Property is Class Field which runtime value would be retrieved at runtime
        /// ValueFunction (no params) is the value result of Class Function that would be retrieved once upfront and stored as Value for processing
        /// ParamFunction is a Class Function with parameters. If parameters do not contain Fields, but just Values, 
        /// the values would be retrieved once upfront and stored as Value for processing
        /// If any Fields are involved, then Value is always retrieved at runtime
        /// Parameters can be Values, Fields and ValueFunctions. Optional future development - adding ParamFunctions as parameters.
        /// </summary>
        public int Ordinal;                     /// Ordinal of the field in the DataLine/DataRow mapped to the constraint
        public string FieldName;                /// name of the field mapped to the constraint
        public string ColumnName;               /// name of the column in target table mapped to the constraint
        //public bool HasNonNullValue;            /// All chacked fields must have at least one value in the file
        public string Expression;              /// full expression,(aka MostRecentAppraisalDate <= @{dateadd(yy, -1, [LoanOriginationDate])}  && MostRecentAppraisalDate != @{} )
        public ErrorLevel ErrorLevel;          /// W, E
//        public List<string> LookupValues;      /// Lookup values or foreign key table
        public ConstraintType ConstraintType;  /// CNType CH, RX, FK, *
        public string Description;             /// Description of the rule for the error messages
        public VRule[] VRules;                 /// List of Single rules x ><!= y
        public string[] Joins;                 /// List of && ||
        public DataColumn DataField;           ///Represents the metadata for the field to which the constraint is mapped.
        public string Print()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("ConstraintType: " + ConstraintType);
            sb.AppendLine("ErrorLevel: " + ErrorLevel);   
            //if (ConstraintType == ConstraintType.LookupValues)
            //    sb.AppendLine(string.Format("Values in ({0}) ", string.Join(",",LookupValues.Take(5))));
            //else
            if (ConstraintType == ConstraintType.Regex)
                sb.AppendLine("Regex: " + Expression);
            if (ConstraintType == ConstraintType.MultiFields)
                sb.AppendLine("Multi-Field check: " + Expression);
            else 
                sb.AppendLine("Check: " + Expression);
            //Console.WriteLine(sb.ToString());
            return sb.ToString();
        }
        public ConstraintEntity()
        {
            VRules = new VRule[0];
            Joins = new string[0];
        }
        public void SplitToRules(string input, DataColumn[] datafields, DataColumn dc)
        {
            //Regex rx = new Regex(RxUtils.strRuleJoin); //@"\s*(?<rule>([^&|]+))\s*(?<join>([&|]?)+)(\s?)*");
            MatchCollection matches = input.ToListOfRuleJoins();// rx.Matches(input);
            List<VRule> rules =  new List<VRule>();
            List<string> joins = new List<string>();
            foreach(Match mm in matches)
            {
                string singlerule = mm.Groups["rule"].Value.ToString();//.Replace("(", "").Replace(")", "");
                string join = mm.Groups["join"].Value.ToString();
                Match vmm = singlerule.ParseAsListOfElements();
                string lval = vmm.Groups["lval"].Value.ToString().Trim();
                string rval = vmm.Groups["rval"].Value.ToString().Trim();
                string comp = vmm.Groups["comp"].Value.ToString();
                VRule vr = new VRule();
                vr.CompOperator = comp;
                vr.leftElement = vr.ParseAsElement(lval, datafields, dc);
                vr.rightElement = vr.ParseAsElement(rval, datafields, dc);
                vr.Expression = singlerule;
                joins.Add(join);
                rules.Add(vr);
            }
            VRules = rules.ToArray();
            Joins = joins.ToArray();
        }   
        public bool IsConstraintValid(string[] datavalues, ref StringBuilder sbldr) 
        {
            bool[] locals = new bool[VRules.Count()];
            for (int i = 0; i < VRules.Count(); i++)
            {
                locals[i] = VRules[i].IsVRuleValid(datavalues, ref sbldr);
            }
            bool b = true;
            if (locals.Count() == 1)
            {
                b = locals[0];
            }
            else 
            if (VRules.Count() > 1)
            {
                for (int i = 1; i < VRules.Count(); i++)
                {
                    b = ExtUtils.join_bool(locals[i - 1], Joins[i - 1], locals[i]);
                }
            }
            return b;
        }

        public ConstraintEntity(string errlevel, string expression, string cntype, string constraint, DataColumn[] dataColumns, DataColumn dcol)
        {
            Expression = expression;
            DataField = dcol;
            Ordinal = dcol.Ordinal + 1;

            SetErrorLevel(errlevel);
            SetConstraintType(cntype);
            if (cntype.ToLower() == "ch")
                SplitToRules(expression, dataColumns, DataField);  

            Description = string.Format("{0}", constraint);
        }  
        public void SetErrorLevel (string errlevel)
        {
            ErrorLevel = errlevel.ToLower() == "e" ? ErrorLevel.Error : ErrorLevel.Warning;
        }
        public void SetConstraintType(string cntype)
        {
            switch(cntype.ToLower())
            {
                case "ch":
                    ConstraintType = ConstraintType.Check;
                    break;
                case "rx":
                    ConstraintType = ConstraintType.Regex;
                    break;
                case "ls":
                case "fk":
                    ConstraintType = ConstraintType.LookupValues;
                    break;
                default:
                    ConstraintType = ConstraintType.Regex;
                    break;
            }
        }
        public class VRule
        {
            public string Expression;
            public RuleElement leftElement;
            public RuleElement rightElement;
            public string CompOperator;
            public bool IsVRuleValid(string[] datavalues, ref StringBuilder sbldr)
            {
                bool b = true;
                object lvalue = leftElement.GetElemRuntimeValue(datavalues, ref sbldr, Program.configR._DEBUG_VERBOSE);
                object rvalue = rightElement.GetElemRuntimeValue(datavalues, ref sbldr, Program.configR._DEBUG_VERBOSE);
                if ((leftElement.ReturnDataType != RuleElement.ReturnType.String
                    && rightElement.ReturnDataType != RuleElement.ReturnType.String)
                    && (lvalue == null || rvalue == null || lvalue.ToString() == "" || rvalue.ToString() == ""))
                    b = false;
                else
                {
                    switch (leftElement.ReturnDataType)
                    {
                        case RuleElement.ReturnType.Integer:
                        case RuleElement.ReturnType.Decimal:
                            b = ExtUtils.compare_two_num(lvalue.ToString() ///decimal.Parse(lvalue.ToString())
                                , CompOperator
                                , rvalue.ToString() /// decimal.Parse(rvalue.ToString())
                                );
                            break;
                        case RuleElement.ReturnType.DateTime:
                        case RuleElement.ReturnType.Date:
                            b = ExtUtils.compare_two_dates(lvalue.ToString() 
                                , CompOperator
                                , rvalue.ToString() 
                                );
                            break;
                        default:
                            b = ExtUtils.compare_two_str(lvalue.ToString() 
                                , CompOperator
                                , rvalue.ToString() 
                                );
                            break;
                    }
                }
                sbldr.AppendLine(string.Format("(({0}) {1}{2}{3} is {4})", leftElement.ReturnDataType.ToString(),
                    lvalue == null ? "NULL" : lvalue.ToString(), CompOperator, rvalue == null ? "NULL" : rvalue.ToString(), b));

                return b;
            }

            //SourceType GetSourceType (string input, DataColumn[] datafields)
            //{
            //    if (input.IsValueFunction())
            //        return SourceType.ValueFunction;                
            //    if (input.IsParamFunction())
            //        return SourceType.ParamFunction;
            //    if (input.IsProperty())                    
            //        return SourceType.Property;
            //    if (input.IsField(datafields)) 
            //        return SourceType.Field;
            //    else
            //        return SourceType.Value;
            //}

            //object GetValueByDataType(object s, DataColumn dc)
            //{
            //    object value;
            //    switch (dc.DataType.ToString())
            //    {
            //        case "System.Int32":
            //            value = Int32.Parse(s.ToString());
            //            break;
            //        case "System.Int64":
            //            value = Int64.Parse(s.ToString());
            //            break;
            //        case "System.Boolean":
            //            value = bool.Parse(s.ToString());
            //            break;
            //        case "System.Decimal":
            //            value = decimal.Parse(s.ToString());
            //            break;
            //        case "System.DateTime":
            //            if ((s.ToString().IsDate()) && (!s.ToString().IsDateTime()))
            //                value = s.ToString().ToDate();
            //            else
            //                value = DateTime.Parse(s.ToString());
            //            break;
            //        default:
            //            value = s.ToString();
            //            break;
            //    }
            //    return value;
            //}
            public RuleElement.Parameter ParseAsParameter(string input, DataColumn[] datafields)
            {
                RuleElement.Parameter p = new RuleElement.Parameter();
                if (input.Trim().StringTrim().IsField(datafields))
                {
                    Match pp = input.Trim().StringTrim().ParseAsField();
                    p.ParamSourceType = SourceType.Field;
                    p.FieldName = pp.Groups["field"].Value.ToString();
                    p.Ordinal = datafields.Where(c => c.ColumnName.ToLower() == pp.Groups["field"].Value.ToString().ToLower()).Select(c => c.Ordinal).FirstOrDefault();
                }
                else
                if (input.Trim().StringTrim().IsValueFunction()) /// Allow only values, fields, or simple value functions as parameters
                {
                    Match mvf = input.Trim().StringTrim().ParseAsValueFunction();
                    string vfunc = mvf.Groups["func"].Value.ToString().Trim().StringTrim();
                    string[] clsarr = vfunc.ParseClassMem();
                    p.ParamSourceType = SourceType.Value;                   
                    p.ClassName = string.Join(".", clsarr.Take(2).ToArray());
                    p.FuncName = clsarr[2];
                    p.Value = Program.CallStaticMethod(p.ClassName, p.FuncName);
                }
                else /// Default is Value type
                {
                    p.Value = (object)input.Trim().StringTrim();
                    p.ParamSourceType = SourceType.Value;
                }
                return p;
            }
            public RuleElement ParseAsElement(string input, DataColumn[] datafields, DataColumn dcol)
            {
                RuleElement elem = new RuleElement();
                elem.Ordinal = dcol.Ordinal;
                elem.ReturnDataType = elem.DataTypeToReturnType(dcol.DataType.ToString());
                Match mm;
                DataColumn dc = new DataColumn();
                string group;
                string[] clsarr;
                if (input.IsField(datafields))
                {
                    group = "field";
                    mm = input.ParseAsField();
                    string fld = mm.Groups[group].Value.ToString().StringTrim();
                    if (fld == "")
                        fld = input.TrimStart('[').TrimEnd(']');
                    dc = datafields.OfType<DataColumn>().Where(c => c.ColumnName.ToLower() == fld.ToLower()).Select(c => c).FirstOrDefault();
                    elem.Ordinal = dc.Ordinal; // datafields.OfType<DataColumn>().Where(c => c.ColumnName.ToLower() == fld.ToLower()).Select(c => c.Ordinal).FirstOrDefault(); 
                    elem.ElemType = ElemType.Field;
                    elem.SourceType = SourceType.Field;                    
                    elem.Name = fld;
                    elem.ReturnDataType = elem.DataTypeToReturnType(dc.DataType.ToString());
                }
                else
                if (input.IsValueFunction())// case SourceType.ValueFunction:
                {
                    group = "func";
                    mm = input.ParseAsValueFunction();
                    clsarr = mm.Groups[group].Value.ToString().ParseClassMem();
                    elem.Name = clsarr[2];
                    elem.ClassName = string.Join(".", clsarr.Take(2).ToArray());
                    elem.ElemType = ElemType.Value;
                    elem.SourceType = SourceType.ValueFunction;
                    elem.Value = Program.CallStaticMethod(elem.ClassName, elem.Name);
                }
                else
                if (input.IsParamFunction())//case SourceType.ParamFunction:
                {
                    group = "func";
                    mm = input.ParseAsParamFunction();
                    clsarr = mm.Groups[group].Value.ToString().ParseClassMem(); //.Split('.').ToArray();
                    elem.Name = clsarr[2]; //ar[ar.Length - 1];
                    elem.ClassName = string.Join(".", clsarr.Take(2).ToArray()); //"csvtosql." + string.Join(".", ar.Take(ar.Length - 2).ToArray());                     
                    object[] pars = mm.Groups["params"].Value.ToString().Split(',').OfType<object>().ToArray();
                    elem.SourceType = SourceType.ParamFunction;
                    int ix = 0;
                    /// /**************************** Parse parameters of the function BEGIN ***********************/
                    List<RuleElement.Parameter> parameters = new List<RuleElement.Parameter>();
                    elem.ElemType = ElemType.Value;
                    elem.SourceType = SourceType.ParamFunction;
                    string[] parDataTypes = Program.GetStaticMethodParameters(elem.ClassName, elem.Name);
                    
                    foreach (var par in pars)
                    {
                        RuleElement.Parameter p = ParseAsParameter(par.ToString(), datafields);
                        if (p.ParamSourceType == SourceType.Field)
                        {
                            p.ParamSourceType = SourceType.Field;
                            elem.ElemType = ElemType.ParamFunction;
                        }
                        else                       
                            switch (parDataTypes[ix])
                            {
                                case "System.Int64":
                                case "System.Int32":
                                case "System.Int16":
                                case "System.Int":
                                    p.Value = int.Parse(p.Value.ToString());
                                    break;
                                case "System.DateTime":
                                    p.Value = DateTime.Parse(p.Value.ToString());
                                    break;
                                case "System.Decimal":
                                case "System.Float":
                                case "System.Real":
                                    p.Value = decimal.Parse(p.Value.ToString());
                                    break;
                                default:
                                    p.Value = p.Value.ToString();
                                    break;
                            }     
                        p.ParamIndex = ix;
                        parameters.Add(p);
                        ix++;
                    }

                    elem.Parameters = parameters.ToArray();
                    /// /**************************** Parse parameters of the function END  *************************/
                    if (elem.ElemType == ElemType.Value)
                        elem.Value = Program.CallStaticMethod(elem.ClassName, elem.Name
                            , elem.Parameters.OfType< RuleElement.Parameter>().Select(p => p.Value).ToArray());
                }
                else
                if (input.IsProperty()) //case SourceType.Property:
                { group = "prop";
                    mm = input.ParseAsProperty();
                    clsarr = mm.Groups[group].Value.ToString().ParseClassMem();
                    elem.Name = clsarr[2]; // ar[ar.Length - 1];
                    elem.ClassName = string.Join(".", clsarr.Take(2).ToArray());
                    elem.Value = Program.GetFieldValue(elem.Name, elem.ClassName);
                }//   break;
                else
                {               
                    elem.ElemType = ElemType.Value;
                    elem.SourceType = SourceType.Value;
                    elem.Value = input;                    
                }

                if (elem.Value != null)
                    switch (elem.ReturnDataType)
                    {
                        case RuleElement.ReturnType.Integer:
                            elem.Value = long.Parse(elem.Value.ToString().StringTrim());
                            break;
                        case RuleElement.ReturnType.Decimal:
                            elem.Value = decimal.Parse(elem.Value.ToString().StringTrim());
                            break;
                        case RuleElement.ReturnType.DateTime:
                            elem.Value = DateTime.Parse(elem.Value.ToString().StringTrim());
                            break;
                        case RuleElement.ReturnType.Date:
                            elem.Value = elem.Value.ToString().StringTrim().ToDate();
                            break;
                        case RuleElement.ReturnType.Boolean:
                            elem.Value = bool.Parse(elem.Value.ToString().StringTrim());
                            break;
                        default:
                            elem.Value = elem.Value.ToString().StringTrim();
                            break;
                    }
                return elem;
            }
           
            public VRule() { }
            public VRule(string singlerule, DataColumn[] dataFields, DataColumn dc) /// DataFIelds 
            {
                Match mm = singlerule.ParseAsListOfElements(); //new Regex(@"\s*(?<lval>\S*)\s*(?<comp>([!<>=]+))\s*(?<rval>\S*)\s*").Match(singlerule);
                CompOperator = mm.Groups["comp"].Value.Length == 0 ? "" : mm.Groups["comp"].Value;
                leftElement = ParseAsElement(mm.Groups["lval"].Value, dataFields, dc);
                rightElement = ParseAsElement(mm.Groups["rval"].Value, dataFields, dc);
                Expression = singlerule;
            }
            public class RuleElement
            {
                //public VRule Parent;
                public SourceType SourceType; 
                public ElemType ElemType;
                //public ElementPosition ElemPosition;
                public int Ordinal; // Belongs to Field only
                public string Name; // Belongs to FieldName, FunctionName  
                public string ClassName; // Belongs to Function only
                public object Value; /// only for value types
                public ReturnType ReturnDataType; // Belongs to Function only 
                public Parameter[] Parameters; // Belongs to Function only 
                public class Parameter
                {
                    public object Value;
                    public string FieldName;
                    public string ClassName;
                    public string FuncName;
                    public int Ordinal;
                    public int ParamIndex;
                    public SourceType ParamSourceType;
                    public bool IsField() { return ParamSourceType == SourceType.Field; }// FieldName != null; }// FieldName.Length > 0; 
             
                    public bool IsFunction
                    {
                        get { return FuncName != null; }// FuncName.Length > 0;

                    }
                    public bool IsValue
                    {
                        get { return Value != null; }// Value.ToString().Length > 0;               
                    }
                }
                public RuleElement() { }

                public ReturnType DataTypeToReturnType(string datatype)
                {
                    ReturnType ReturnDataType;
                    switch(datatype)
                    {
                        case "integer":
                        case "System.Int32":
                        case "System.Int64":
                        case "System.Int16":
                        case "System.Int":
                        case "System.Long":
                        case "int":
                        case "long":
                        case "short": 
                            ReturnDataType = ReturnType.Integer;
                            break;
                        case "Date":
                        case "System.DateTime":
                        case "datetime":
                        case "date":       
                            ReturnDataType = ReturnType.DateTime;
                            break;
                        case "double":
                        case "System.Decimal":
                        case "System.Real":
                        case "System.Float":
                        case "System.Double":
                        case "decimal":
                        case "float":
                        case "real":
                            ReturnDataType = ReturnType.Decimal;
                            break;
                        case "bool":
                        case "System.Boolean":
                        case "bit":
                            ReturnDataType = ReturnType.Boolean;
                            break;
                        default:
                            ReturnDataType = ReturnType.String;
                            break;


                    }
                    return ReturnDataType;
                }

                public object GetElemRuntimeValue(string[] datavalues, ref StringBuilder sbldr, bool debug_verbose) 
                {
                    object value;
                    if (!debug_verbose)
                    {
                        if (ElemType == ElemType.Value)
                        {
                            return Value;
                        }
                        else
                        if (ElemType == ElemType.Field)
                        {
                            value = datavalues.GetValue(Ordinal);
                            return value;
                        }
                        else
                        if (ElemType == ElemType.ValueFunction)
                        {
                            if (Value == null)
                            {
                                value = Program.CallStaticMethod(ClassName, Name);
                                Value = value;
                            }
                            return Value;
                        }
                        else
                        if (ElemType == ElemType.ParamFunction)
                        {
                            foreach (RuleElement.Parameter p in Parameters)
                            {
                                if (p.ParamSourceType == SourceType.Field)
                                {
                                    p.Value = datavalues.GetValue(p.Ordinal);
                                }
                            }
                            value = Program.CallStaticMethod(ClassName, Name, Parameters.OfType<Parameter>().Select(p => p.Value).ToArray());
                            return value;
                        }
                        else
                        if (ElemType == ElemType.Property)
                        {
                            if (Value == null)
                            {
                                value = Program.GetFieldValue(ClassName, Name);
                                Value = value;
                            }
                            return Value;
                        }
                        else
                        {                         
                            return Value;
                        }
                    }
                    else
                    {
                        sbldr.AppendLine(string.Format("Initial Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                        if (ElemType == ElemType.Value)
                        {
                            return Value;
                        }
                        else
                        if (ElemType == ElemType.Field)
                        {
                            value = datavalues.GetValue(Ordinal);
                            sbldr.AppendLine(string.Format("Field RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                            return value;
                        }
                        else
                        if (ElemType == ElemType.ValueFunction)
                        {
                            if (Value == null)
                            {
                                value = Program.CallStaticMethod(ClassName, Name);
                                Value = value;
                                sbldr.AppendLine(string.Format("Function RT Value: ({0}) {1}", ReturnDataType.ToString()
                                    , value != null ? value.ToString() : "NULL"));
                            }
                            else                                
                                sbldr.AppendLine(string.Format("Function Stored Value: ({0}) {1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                            return Value;
                        }
                        else
                        if (ElemType == ElemType.ParamFunction)
                        {
                            foreach (RuleElement.Parameter p in Parameters)
                            {
                                if (p.ParamSourceType == SourceType.Field)
                                {
                                    p.Value = datavalues.GetValue(p.Ordinal);
                                    sbldr.AppendLine(string.Format("Parameter (Field) RT Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                                }
                                else
                                    sbldr.AppendLine(string.Format("Parameter Stored Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                            }
                            value = Program.CallStaticMethod(ClassName, Name, Parameters.OfType<Parameter>().Select(p => p.Value).ToArray());
                            sbldr.AppendLine(string.Format("ParamFunction RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                            return value;
                        }
                        else
                        if (ElemType == ElemType.Property)
                        {
                            if (Value == null)
                            {
                                value = Program.GetFieldValue(ClassName, Name);
                                Value = value;
                                sbldr.AppendLine(string.Format("Property RT Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                            }
                            else
                                sbldr.AppendLine(string.Format("Property Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                            return Value;
                        }
                        else
                        {
                            sbldr.AppendLine(string.Format("Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                            return Value;
                        }
                    }
                    //if (debug_verbose)
                    //    sbldr.AppendLine(string.Format("Initial Value: ({0}){1}", ReturnDataType.ToString(),Value != null ? Value.ToString() : "NULL"));
                    ////get { 
                    //if (ElemType == ElemType.Value)
                    //{
                    //    return Value;
                    //}
                    //else
                    //if (ElemType == ElemType.Field)
                    //{
                    //    value = datavalues.GetValue(Ordinal);
                    //    if (debug_verbose)
                    //        sbldr.AppendLine(string.Format("Field RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                    //    return value;
                    //}
                    //else
                    //if (ElemType == ElemType.ValueFunction)
                    //{
                    //    if (Value == null)
                    //    {
                    //        value = Program.CallStaticMethod(ClassName, Name);
                    //        Value = value;
                    //        if (debug_verbose)
                    //            sbldr.AppendLine(string.Format("Function RT Value: ({0}) {1}", ReturnDataType.ToString()
                    //            , value != null ? value.ToString() : "NULL"));
                    //    }
                    //    else
                    //        if (debug_verbose)
                    //            sbldr.AppendLine(string.Format("Function Stored Value: ({0}) {1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));

                    //    return Value;                         
                    //}
                    //else
                    //if (ElemType == ElemType.ParamFunction)
                    //{
                    //    foreach (RuleElement.Parameter p in Parameters)
                    //    {
                    //        if (p.ParamSourceType == SourceType.Field)
                    //        {
                    //            p.Value = datavalues.GetValue(p.Ordinal);
                    //            if (debug_verbose)
                    //                sbldr.AppendLine(string.Format("Parameter (Field) RT Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                    //        }
                    //        else
                    //            if (debug_verbose)
                    //                sbldr.AppendLine(string.Format("Parameter Stored Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                    //    }
                    //    value = Program.CallStaticMethod(ClassName, Name, Parameters.OfType<Parameter>().Select(p => p.Value).ToArray());
                    //    if (debug_verbose)
                    //        sbldr.AppendLine(string.Format("ParamFunction RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                    //    return value;
                    //}
                    //else
                    //if (ElemType == ElemType.Property)
                    //{
                    //    if (Value == null)
                    //    {
                    //        value = Program.GetFieldValue(ClassName, Name);
                    //        Value = value;
                    //        if (debug_verbose)
                    //            sbldr.AppendLine(string.Format("Property RT Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                    //    }
                    //    else
                    //        if (debug_verbose)
                    //            sbldr.AppendLine(string.Format("Property Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                    //    return Value;
                    //}
                    //else
                    //{
                    //    if (debug_verbose)
                    //        sbldr.AppendLine(string.Format("Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                    //    return Value;
                    //}
                }
                public object GetElemRuntimeValue(string[] datavalues, ref StringBuilder sbldr)
                {
                    object value;
                    if (ElemType == ElemType.Value)
                    {
                        return Value;
                    }
                    else
                    if (ElemType == ElemType.Field)
                    {
                        value = datavalues.GetValue(Ordinal);
                        //if (debug_verbose)
                        //    sbldr.AppendLine(string.Format("Field RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                        return value;
                    }
                    else
                    if (ElemType == ElemType.ValueFunction)
                    {
                        if (Value == null)
                        {
                            value = Program.CallStaticMethod(ClassName, Name);
                            Value = value;
                            //if (debug_verbose)
                            //    sbldr.AppendLine(string.Format("Function RT Value: ({0}) {1}", ReturnDataType.ToString()
                            //    , value != null ? value.ToString() : "NULL"));
                        }
                        //else
                        //    if (debug_verbose)
                        //    sbldr.AppendLine(string.Format("Function Stored Value: ({0}) {1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));

                        return Value;
                    }
                    else
                    if (ElemType == ElemType.ParamFunction)
                    {
                        foreach (RuleElement.Parameter p in Parameters)
                        {
                            if (p.ParamSourceType == SourceType.Field)
                            {
                                p.Value = datavalues.GetValue(p.Ordinal);
                                //if (debug_verbose)
                                //    sbldr.AppendLine(string.Format("Parameter (Field) RT Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                            }
                            //else
                            //    if (debug_verbose)
                            //    sbldr.AppendLine(string.Format("Parameter Stored Value: ({0}){1}", ReturnDataType.ToString(), p.Value != null ? p.Value.ToString() : "NULL"));
                        }
                        value = Program.CallStaticMethod(ClassName, Name, Parameters.OfType<Parameter>().Select(p => p.Value).ToArray());
                        //if (debug_verbose)
                        //    sbldr.AppendLine(string.Format("ParamFunction RT Value: ({0}){1}", ReturnDataType.ToString(), value != null ? value.ToString() : "NULL"));
                        return value;
                    }
                    else
                    if (ElemType == ElemType.Property)
                    {
                        if (Value == null)
                        {
                            value = Program.GetFieldValue(ClassName, Name);
                            Value = value;
                            //if (debug_verbose)
                            //    sbldr.AppendLine(string.Format("Property RT Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                        }
                        //else
                        //    if (debug_verbose)
                        //    sbldr.AppendLine(string.Format("Property Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                        return Value;
                    }
                    else
                    {
                        //if (debug_verbose)
                        //    sbldr.AppendLine(string.Format("Stored Value: ({0}){1}", ReturnDataType.ToString(), Value != null ? Value.ToString() : "NULL"));
                        return Value;
                    }
                }
                public enum ReturnType
                {
                    String = 0,
                    Integer = 1,
                    Decimal = 2, 
                    DateTime = 3,
                    Date = 4,
                    Boolean = 5,
                    Object = 6
                }
                //public object RuntimeValue(string[] datafields)
                //{
                //    if (ParamSourceType == SourceType.Value)
                //        return Value;
                //    else
                //    if (ParamSourceType == SourceType.ValueFunction)
                //    {
                //        if (Value == null)
                //            Value = Program.CallStaticMethod(ClassName, FuncName);
                //        return Value;
                //    }
                //    else
                //    if (ParamSourceType == SourceType.Field)
                //        return datafields.GetValue(Ordinal);
                //    else
                //    if (ParamSourceType == SourceType.Property)
                //    {
                //        if (Value == null)
                //            Value = Program.GetFieldValue(FieldName, ClassName);
                //        return Value;
                //    }
                //    /// this option is NOT available
                //    else
                //    if (ParamSourceType == SourceType.ParamFunction)
                //        return Value; // - not developed that yet Program.CallStaticMethod(ClassName, FuncName, Pa;
                //    else
                //        return Value;
                //}
                //public void SetInitValue(object value)
                //{
                //    if (ParamSourceType == SourceType.Value)
                //        Value = value;
                //    else
                //    if (ParamSourceType == SourceType.ValueFunction)
                //        Value = Program.CallStaticMethod(ClassName, FuncName);
                //    else
                //    if (ParamSourceType == SourceType.Property)
                //        Value = Program.GetFieldValue(FieldName, ClassName);
                //    /// this option is NOT available
                //    else
                //        Value = value;
                //}
                //public RuleElement(string functionName, object[] parameters)
                //{
                //    Name = functionName;

                //    Ordinal = -1;
                //    if (parameters.Length == 0)
                //    {
                //        SourceType = SourceType.ValueFunction;
                //        Parameters = new Parameter[] { };
                //    }
                //    else
                //    {
                //        SourceType = SourceType.ParamFunction;
                //        // parse each parameter: first as field, then simple function , then value , for now skip param function
                //        foreach (object o in parameters)
                //        {

                //            Parameters.Concat(new List<Parameter>() { (Parameter)o });
                //        }

                //    }
                //    /// Get ReturnDataType from code 
                //    /// 
                //}
                //public void SetElemInitValue(object value) 
                //{
                //    if (ElemType == ElemType.Value)
                //        Value = value;
                //    else
                //    if (ElemType == ElemType.ValueFunction)
                //    {
                //        Value = Program.CallStaticMethod(ClassName, Name);
                //    }
                //    else
                //    if (ElemType == ElemType.ParamFunction)
                //    {
                //        if (Parameters.Where(p => p.IsField()).Count() == 0)
                //            Value = Program.CallStaticMethod(ClassName, Name, Parameters);
                //    }
                //    else
                //    if (ElemType == ElemType.Property)
                //    {
                //        Value = Program.GetFieldValue(ClassName, Name);
                //    }
                //    else
                //        Value = value;
                //    /// Convert to the datatype
                //    switch(ReturnDataType)
                //    {
                //        case ReturnType.Integer:
                //            Value = long.Parse(Value.ToString());
                //            break;
                //        case ReturnType.Decimal:
                //            Value = decimal.Parse(Value.ToString());
                //            break;
                //        case ReturnType.Date:
                //            Value = (Value.ToString()).ToDate();
                //            break;
                //        case ReturnType.DateTime:
                //            Value = DateTime.Parse(Value.ToString());
                //            break;
                //        case ReturnType.Boolean:
                //            Value = bool.Parse(Value.ToString());
                //            break;
                //        case ReturnType.String:
                //            Value = bool.Parse(Value.ToString());
                //            break;
                //        default:
                //            break;
                //    }

                //}
                //public RuleElement(object value)
                //{
                //    SetElemInitValue(value);
                //}    
                //public RuleElement(string fieldName, int ordinal, string datatype)
                //{
                //    Name = fieldName;
                //    Ordinal = ordinal;
                //    SourceType = SourceType.Field;
                //    if (datatype == "integer")
                //        ReturnDataType = ReturnType.Integer;
                //    else
                //    if (datatype == "date")
                //        ReturnDataType = ReturnType.Date;
                //    else
                //    if (datatype == "datetime")
                //        ReturnDataType = ReturnType.DateTime;
                //    else
                //    if (datatype == "decimal")
                //        ReturnDataType = ReturnType.Decimal;
                //    else
                //        ReturnDataType = ReturnType.String;
                //}
            }

        }
    }
    enum ElemType
    {
        Value = 0,
        Field = 1,
        ValueFunction = 2,
        ParamFunction = 3,
        Property = 4
    }
    enum ConstraintType
    {
        Check = 0,
        Regex = 1,
        LookupValues = 2,
        MultiFields = 3
    }
    public enum SourceType
    {
        Value = 0,
        Field = 1,
        ValueFunction = 2,
        ParamFunction = 3, 
        Property = 4
    }
}

