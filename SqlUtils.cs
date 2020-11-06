using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Collections;
using System.IO;
using System.Threading;

namespace csvtosql
{ 
    public class SqlUtils  
    {
        public string _SERVER;
        public string _DATABASE;
        public string _TABLE;
        public string _WARN_TABLE; 
        public string _LOG_TABLE = @"[dbo].[error_log]";
        public string _META_TABLE;
        public string _WARN_CMD_STMT= "";
        public readonly string _PARAMS = @"@table nvarchar(max)";
        public string _WARN_PARAMS="";
        public string _WARN_COLS="";
        public DataTable _localWARN;
        public List<int> _CheckAllNulls;
        public string _WARN_BASE_PARAMS =  @"@column_name varchar(250), @value nvarchar(4000),@fail_reason nvarchar(4000),@line_num bigint,@date datetime,@dvid bigint" ;
        public string _WARN_BASE_COLS = @"column_name,value,fail_reason,line_num,date,dvid";
        public readonly string _PARAMS_ERR = @"@line_id bigint,@msg nvarchar(4000),@line nvarchar(max),@datafile nvarchar(4000), @dvid bigint";
        readonly string _SELECT = @"select distinct c.column_id as FIELDID
		,c.name as FIELD
		,t.name as DATA_TYPE
		,ISFIELD = 1
		,ISATTRIB = 0
		,ISPK =  iif(ixcol.column_id is null, 0,1) 
		,MAX_LENGTH = c.max_length
		,TOTAL_DIGITS = c.precision
		,FRACTION_DIGITS = c.scale 
from sys.tables o (nolock) 
join sys.columns c (nolock) on o.object_id = c.object_id
join sys.types t (nolock) on c.user_type_id = t.user_type_id
left join sys.indexes ix on o.object_id = ix.object_id and ix.is_primary_key = 1
left join sys.index_columns ixcol on ix.object_id = ixcol.object_id and ix.index_id = ixcol.index_id and ixcol.column_id = c.column_id
where t.name !='sysname'
and o.name not like 'sys%'   
and o.name not like 'dt%'
and o.object_id = object_id(@table)";
        //public char Delimiter()
        //{
        //    return Char.Parse(_LocalDS.Tables["Table"].Rows[0]["delimiter"].ToString());
        //}
        public List<string> _PK_COLS;
        public DataSet _LocalDS;
        public List<SqlParameter> _SQL_PARAMETERS;
        public List<string> _dsERRORS;
        public SqlUtils()
        {            
            _LocalDS = new DataSet();
            _SQL_PARAMETERS = new List<SqlParameter>();
        }
        public SqlUtils(string server, string database)
        {
            _SERVER = server;
            _DATABASE = database;
            _LocalDS = new DataSet();
            _SQL_PARAMETERS = new List<SqlParameter>();
        }
        public string GetSqlConnStr()
        {
            string connstr = "Data Source=" + _SERVER + ";Initial Catalog=" + _DATABASE + ";Integrated Security=SSPI"
                + ";MultipleActiveResultSets=True;Connection Timeout=600";
            return connstr;
        }
        public void GetLocalDataSet(string filename)
        {   
            SqlCommand cmd = new SqlCommand();
            //cmd.CommandText = _META_TABS;
            DataSet tgt_ds = new DataSet();
            _dsERRORS = new List<string>();

            //SqlConnection conn = new SqlConnection(GetSqlConnStr());
            //DataTable table = new DataTable();

            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                SqlCommand sqlComm = new SqlCommand("dv.sp_get_meta_data_set", conn);
                sqlComm.Parameters.AddWithValue("@filename", filename);
                sqlComm.CommandType = CommandType.StoredProcedure;
                SqlDataAdapter da = new SqlDataAdapter("dv.sp_get_meta_data_set", conn)
                {
                    SelectCommand = sqlComm
                };
                ///da.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                da.Fill(tgt_ds);
                if (tgt_ds.Tables.Count > 0)
                    tgt_ds.Tables[0].TableName = "Table";           /// Blank copy of the target table
                if (tgt_ds.Tables.Count > 1)
                    tgt_ds.Tables[1].TableName = "FileInfo";        /// Single row table/file info
                if (tgt_ds.Tables.Count > 2)
                    tgt_ds.Tables[2].TableName = "TableSchema";     /// File Table/Fields schema
                if (tgt_ds.Tables.Count > 3)
                    tgt_ds.Tables[3].TableName = "Rules";           /// Rules
                if (tgt_ds.Tables.Count > 4)
                    tgt_ds.Tables[4].TableName = "Relations";       /// Relations 
                if (tgt_ds.Tables.Count > 5)
                    tgt_ds.Tables[5].TableName = "Config";          /// Config(uration) settings overwriting the file.ini
                //}
                if ((tgt_ds.Tables.Count == 0) || (tgt_ds.Tables["FileInfo"].Rows.Count == 0) || (tgt_ds.Tables["TableSchema"].Rows.Count == 0))
                {
                    _dsERRORS.Add(string.Format("E: No metadata found for file {0}.", filename));
                    throw new CustomException(string.Format("E: No metadata found for file {0}.", filename));
                }
                /// Create Foreign Keys if any
                foreach (DataRow drow in tgt_ds.Tables["Relations"].Rows)
                {
                    try
                    {   //da.SelectCommand = new SqlCommand(drow["selectcmd"].ToString(), conn);
                        using (da = new SqlDataAdapter(drow["selectcmd"].ToString(), conn))
                        {
                            string tab = drow["ref_table_name"].ToString();
                            string colname = drow["field"].ToString();
                            string ref_colname = drow["ref_colname"].ToString();
                            int ordinal = Int32.Parse(drow["ordinal"].ToString()) - 1;
                            int ref_table_id = int.Parse(drow["ref_table_id"].ToString());
                            if (ref_table_id == 0)
                            {
                                _dsERRORS.Add(string.Format("W: Field {0} missing lookup table|list {1}[{2}]", colname, tab, ref_colname));
                            }
                            else
                            {

                                //{
                                //    Console.WriteLine(string.Format("By column: {0,20}\tselectcmd: {1}", drow["colname"].ToString(), drow["selectcmd"].ToString()));
                                //    Console.WriteLine(string.Format("By ordinal: {0,20}\tselectcmd: {1}", tgt_ds.Tables[0].Columns[ordinal].ToString(), drow["selectcmd"].ToString()));
                                //}
                                da.Fill(tgt_ds, tab);

                                tgt_ds.Tables[tab].Columns[ref_colname].Unique = true;// new DataColumn[] { tgt_ds.Tables[tab].Columns[0] };
                                                                                      //tgt_ds.Tables[tab].PrimaryKey = new DataColumn[] { tgt_ds.Tables[tab].Columns[0] };
                                ForeignKeyConstraint fk = new ForeignKeyConstraint(drow["fk_name"].ToString(), tgt_ds.Tables[tab].Columns[0], tgt_ds.Tables[0].Columns[colname])
                                {
                                    AcceptRejectRule = AcceptRejectRule.None,
                                    DeleteRule = Rule.None,
                                    UpdateRule = Rule.None
                                };
                                //tgt_ds.Tables[0].Constraints.Add(new ForeignKeyConstraint(drow["fk_name"].ToString(), tgt_ds.Tables[tab].Columns[0], tgt_ds.Tables[0].Columns[colname]));
                                tgt_ds.Tables[0].Constraints.Add(fk);
                            }
                        }
                    }
                    catch (CustomException e)
                    {
                        _dsERRORS.Add(e.Message);
                    }
                    catch (SqlException e)
                    {
                        _dsERRORS.Add("E: "+ e.Message);
                    }
                    catch (Exception e)
                    {
                        _dsERRORS.Add("E: " + e.Message);                        
                    }
                }
            }
            List<DataColumn> keys = new List<DataColumn>();
            List<SqlParameter> pars = new List<SqlParameter>();

            foreach (DataRow dr in tgt_ds.Tables["TableSchema"].Rows)
            {
                try
                {   /// catch errors related to table schema
                    string colname = dr["field"].ToString();
                    if (tgt_ds.Tables["Table"].Columns.OfType<DataColumn>().Where(c => c.ColumnName == colname).Count() > 0)
                    {
                        string data_type_name = dr["data_type"].ToString();
                        int max_length = int.Parse(dr["max_length"].ToString());
                        int precision = int.Parse(dr["precision"].ToString());
                        int scale = int.Parse(dr["scale"].ToString());
                        bool is_nullable =  dr["is_nullable"].ToString().ToBit();
                        int keyindex = int.Parse(dr["keyindex"].ToString());
                        int uniqueindex = int.Parse(dr["uniqueindex"].ToString());
                        bool is_primary_key = uniqueindex > 0 || keyindex > 0 ? true : false;
                        /// Get Datacolumn ref
                        DataColumn dc = tgt_ds.Tables["Table"].Columns[colname];                     

                        if ((precision == 0) && (scale == 0) && (dc.DataType == typeof(string)))
                            dc.MaxLength = max_length;
                        dc.AllowDBNull = is_nullable;
                        dc.Caption = dr["colname"].ToString();

                        if (is_primary_key)
                        {
                            pars.Add(new SqlParameter(dr["colname"].ToString(), SqlDbType.NVarChar));
                            keys.Add(dc);
                        }
                    }
                }
                catch (CustomException e)
                {                  
                    _dsERRORS.Add(e.Message);
                }
                catch (SqlException e)
                {
                    _dsERRORS.Add("E: " + e.Message);
                }
                catch (Exception e)
                {
                    _dsERRORS.Add("E: " + e.Message);
                }
            }

            /// PrimayKey or UniqueKey creation, if any keys 
            var pks = tgt_ds.Tables["TableSchema"].Rows.OfType<DataRow>().Where(drow => (int)drow["uniqueindex"] > 0).OrderBy(drow => drow["uniqueindex"])
                .Select(drow => tgt_ds.Tables[0].Columns[drow["field"].ToString()]).ToArray();
            if (pks.Length > 0)
                tgt_ds.Tables[0].PrimaryKey = pks;
                  

            /// Once Done, Build Logging facilities:
            /// '@column_name varchar(250),@value nvarchar(4000),@fail_reason nvarchar(4000),@date datetime,@line_num bigint'
            pars.Add(new SqlParameter("column_name", SqlDbType.VarChar, 250));
            pars.Add(new SqlParameter("value", SqlDbType.NVarChar, 4000));
            pars.Add(new SqlParameter("fail_reason", SqlDbType.NVarChar, 4000));
            pars.Add(new SqlParameter("line_num", SqlDbType.BigInt));
            pars.Add(new SqlParameter("date", SqlDbType.DateTime)); 
            //SqlParameter pdvid = new SqlParameter("dvid", SqlDbType.BigInt);
            //pdvid.Value = long.Parse(tgt_ds.Tables["FileInfo"].Rows[0]["dvid"].ToString());
            //pars.Add(pdvid);

            /// Initialize interenal params
            /// 
            _PK_COLS = pks.Select(k => k.Caption.ToString()).ToList();
            _localWARN = new DataTable();
            _localWARN.Columns.AddRange(_PK_COLS.Select(c => new DataColumn(c, typeof(string))).ToArray());
            _localWARN.Columns.Add(new DataColumn("column_name", typeof(string)));
            _localWARN.Columns.Add(new DataColumn("value", typeof(string)));
            _localWARN.Columns.Add(new DataColumn("fail_reason", typeof(string)));
            _localWARN.Columns.Add(new DataColumn("line_num", typeof(long)));
            DataColumn dt = new DataColumn("date", typeof(DateTime));
            dt.DefaultValue = DateTime.Now;
            _localWARN.Columns.Add(dt);
            DataColumn dvid = new DataColumn("dvid", typeof(string));
            dvid.DefaultValue = tgt_ds.Tables["FileInfo"].Rows[0]["dvid"].ToString();
            _localWARN.Columns.Add(dvid);

            _SQL_PARAMETERS = pars;

            _WARN_COLS = string.Join(",", pks.Select(k => k.Caption.ToString().Quotename(@"["))) + @",[column_name],[value],[fail_reason],[line_num],[date],[dvid]";
            /// Create WARN insert params for sp_executesql
            _WARN_PARAMS = string.Join(",", pks.Select(k => @"@" + k.Caption.ToString() + " nvarchar(4000)")) 
                + @",@column_name varchar(250), @value nvarchar(4000),@fail_reason nvarchar(4000),@line_num bigint,@date datetime";

            /// Set SqlInsert Warn command
            _WARN_CMD_STMT = string.Format("insert into {0} ({1}) values ({2}{3})"
                , tgt_ds.Tables["FileInfo"].Rows[0]["warn_table"].ToString()
                , string.Join(",", pks.Select(k => k.Caption.ToString().Quotename(@"["))) + @",[column_name],[value],[fail_reason],[line_num],[date],[dvid]"
                , string.Join(",", pks.Select(k => @"@" + k.Caption.ToString())) + @",@column_name,@value,@fail_reason,@line_num, @date,"
                , tgt_ds.Tables["FileInfo"].Rows[0]["dvid"].ToString());

            /// File info


            /// Create CheckConstraint if any (FUTURE)
            /// Add Rules if any (FUTURE)
            List<int> ordinals0 = tgt_ds.Tables["Rules"].Rows.OfType<DataRow>()
                    .Where(r => int.Parse(r["skip_all_null_check"].ToString()) == 0)
                    .Select(r => int.Parse(r["ordinal"].ToString())).ToList();
            ordinals0.AddRange(tgt_ds.Tables["Relations"].Rows.OfType<DataRow>()
                    .Where(r => int.Parse(r["skip_all_null_check"].ToString()) == 0)
                    .Select(r => int.Parse(r["ordinal"].ToString())).ToList().Distinct());
            List<int> rl_ordinals0 = new List<int>();
            foreach (int ordinal in ordinals0.OrderBy(o => o))
                if (!rl_ordinals0.Contains(ordinal))
                    rl_ordinals0.Add(ordinal-1);
            _CheckAllNulls = rl_ordinals0;

            List<int> ordinals = tgt_ds.Tables["Rules"].Rows.OfType<DataRow>()              
                .Select(r => int.Parse(r["ordinal"].ToString())).ToList();
            ordinals.AddRange(tgt_ds.Tables["Relations"].Rows.OfType<DataRow>()
                .Select(r => int.Parse(r["ordinal"].ToString())).ToList().Distinct());
            List<int> rl_ordinals = new List<int>();
            foreach (int ordinal in ordinals.OrderBy(o => o))
                if (!rl_ordinals.Contains(ordinal))
                    rl_ordinals.Add(ordinal);
            
            foreach (int ordinal in rl_ordinals.OrderBy(o => o))
            {
                StringBuilder sb_err = new StringBuilder();
                try /// Catch all errors related to creation of Rules 
                {
                    DataColumn rcol = tgt_ds.Tables["Table"].Columns[ordinal-1];
                    List<ConstraintEntity> rules = new List<ConstraintEntity>();
                    /// Create a list of rules
                    var all_rules = tgt_ds.Tables["Rules"].Rows.OfType<DataRow>()
                        .OrderBy(r => int.Parse(r["ordinal"].ToString()))
                        .ThenBy(r => r["cntype"]);
                    foreach (DataRow dr in all_rules.OfType<DataRow>()
                        .Where(r => ordinal == int.Parse(r["ordinal"].ToString()) && r["cntype"].ToString() != "fk")
                        .Select(r => r).ToArray())//.Where(r => r.Field<string>("cntype") == "ch").Select(r => r))
                    {   /// Check rules     
                        //if (ordinal == int.Parse(dr["ordinal"].ToString())-1)
                        //{
                        sb_err.AppendFormat("{0}\t{1}\t{2}\t{3}\t{4}", ordinal, dr["cntype"].ToString().Quotename("'"), dr["expression"].ToString().Quotename("'")
                            , string.Join(",", tgt_ds.Tables["Table"].Columns.OfType<DataColumn>().Select(c => c.ColumnName.Quotename("'")).ToArray())
                            , dr["constraint"].ToString());
                            ConstraintEntity rl = new ConstraintEntity(dr["errlevel"].ToString()
                                , dr["expression"].ToString(), dr["cntype"].ToString(), dr["constraint"].ToString()
                                , tgt_ds.Tables["Table"].Columns.OfType<DataColumn>().ToArray(), rcol);
                            //rl.Description = string.Format("{0}: {1}", dr["errlevel"].ToString(), dr["constraint"].ToString());
                            rl.Ordinal = ordinal;
                            rl.DataField = rcol;
                            rl.DataField.DataType = rcol.DataType;                                                      
                            rl.FieldName = rcol.ColumnName;
                            rl.ColumnName = rcol.Caption;
                            rules.Add(rl);                    
                        //}                        
                    }

                    rcol.ExtendedProperties.Add("ch", rules);
                    
                    foreach (DataRow dr in tgt_ds.Tables["Relations"].Rows.OfType<DataRow>()
                    .Where(r => ordinal == int.Parse(r["ordinal"].ToString())))
                    {            
                        string ref_table_name = dr["ref_table_name"].ToString();
                        string ref_colname = dr["ref_colname"].ToString();

                        sb_err.Append(string.Format("{0}\t{1}\t|{2}|\t{3}\t{4}", ordinal, dr["field"].ToString().Quotename("'")
                            , dr["table_name"].ToString().Quotename("'")
                            , ref_table_name.Quotename("'")
                            , ref_colname.Quotename("'")));

                        if (//(ordinal == int.Parse(dr["ordinal"].ToString())-1) && 
                        (tgt_ds.Tables[ref_table_name] != null))
                        {

                            // ValidationRule fk = new ValidationRule("fk"
                            List<string>  lst = tgt_ds.Tables[ref_table_name].Rows.OfType<DataRow>().Select(r => r[ref_colname].ToString()).ToList();
                            //fk.Description = "Lookup list";
                            rcol.ExtendedProperties.Add("fk", lst);
                            sb_err.Append(string.Format("{0}\t{1}\t|{2}|\t{3}\t{4}", ordinal, dr["field"].ToString().Quotename("'") + " added"
                                , dr["table_name"].ToString().Quotename("'")
                                , ref_table_name.Quotename("'")
                                , ref_colname.Quotename("'")));
                        }
                    }
                }                    
                catch (Exception e)
                {
                    _dsERRORS.Add(string.Format("E: {0}\n{1}", e.Message, sb_err.ToString()));
                    continue;
                }       
            }
            _LocalDS = tgt_ds;
            //return tgt_ds;
        }
        public Dictionary<string, string> ReadConfigTable(string configtable, Dictionary<string, string> config)
        {
            //Dictionary<string, string> config = new Dictionary<string, string>();
            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                SqlCommand cmd = new SqlCommand();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar));
                StringBuilder sb = new StringBuilder();
                cmd.Parameters[0].Value = string.Format("select conf_id, conf_variable, conf_value, conf_attr, conf_descr " +
                    " from {0}(nolock) " +
                    " where is_active=1"
                    , configtable);
                SqlDataReader dr = cmd.ExecuteReader();
                while (dr.Read())
                {
                    config.Add(dr["conf_variable"].ToString(), dr["conf_value"].ToString());
                }

            }

            return config;
        }
        class DataField : DataColumn
        {
            DataColumn Field;
            //List<ConstraintEntity> Rules;
            List<string> ValuesStr;
            List<int> ValuesInt32;
            List<long> ValuesInt64;
            List<decimal> ValuesDec;
            public DataField() { }
           
            public DataField(DataColumn dc)
            {
                Field = dc;
            }
            public DataField(DataColumn dc, List<ConstraintEntity> rules)
            {
                Field = dc;
                //Rules = rules;
            }
            public DataField(DataColumn dc, IEnumerable<object> values) 
            {
                Field = dc;
                if (Field.DataType == typeof(int))
                    ValuesInt32 = values.Select(v => int.Parse(v.ToString())).ToList();
                else
                if (Field.DataType == typeof(long))
                    ValuesInt64 = values.Select(v => long.Parse(v.ToString())).ToList();
                else
                if (Field.DataType == typeof(decimal))
                    ValuesDec = values.Select(v => decimal.Parse(v.ToString())).ToList();
                else
                    ValuesStr = values.Select(v => v.ToString()).ToList();
            }
        }
        public DataTable ReadTargetSchema()
        {
            DataTable dtab = GetDictionaryTable("DestTable");
            SqlConnection conn = new SqlConnection(GetSqlConnStr());
            SqlCommand cmd = new SqlCommand();
            conn.Open();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = @"sp_executesql";// this.GetSelect();
            cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)).Value = _SELECT;
            cmd.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar)).Value = _PARAMS;
            cmd.Parameters.Add(new SqlParameter("table", SqlDbType.NVarChar)).Value = _TABLE;
            SqlDataReader dr = cmd.ExecuteReader();
                // SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                DataRow drow = dtab.NewRow();
                for (int ix = 0; ix < dtab.Columns.Count; ix++)
                {
                    //Console.WriteLine(string.Format("{0}\t{1}", dtab.Columns[ix].ColumnName, dtab.Columns[ix].DataType));
                    switch (dtab.Columns[ix].DataType.ToString())
                    {
                        case "System.Int32":
                            drow[ix] = Int32.Parse(dr[ix].ToString());
                            break;
                        case "System.Int64":
                            drow[ix] = Int64.Parse(dr[ix].ToString());
                            break;
                        case "System.Boolean":
                            drow[ix] = bool.Parse(dr[ix].ToString());
                            break;
                        case "System.Decimal":
                            drow[ix] = decimal.Parse(dr[ix].ToString());
                            break;
                        default:
                            drow[ix] = dr[ix].ToString();
                            break;
                    }
                }
                dtab.Rows.Add(drow);
            }
            dr.Close();
            return dtab;
        }
        public DataTable GetDictionaryTable(string tabname)
        {
            DataTable dt = new DataTable(tabname);

            //dt.Columns.Add(new DataColumn("TABLE", typeof(String)));
            dt.Columns.Add(new DataColumn("FIELDID", typeof(Int32)));
            dt.Columns.Add(new DataColumn("FIELD", typeof(String)));
            dt.Columns.Add(new DataColumn("DATA_TYPE", typeof(String)));
            dt.Columns.Add(new DataColumn("ISFIELD", typeof(Int32)));
            dt.Columns.Add(new DataColumn("ISATTRIB", typeof(Int32)));
            dt.Columns.Add(new DataColumn("ISPK", typeof(Int32)));
            dt.Columns.Add(new DataColumn("MAX_LENGTH", typeof(Int32)));
            dt.Columns.Add(new DataColumn("TOTAL_DIGITS", typeof(Int32)));
            dt.Columns.Add(new DataColumn("FRACTION_DIGITS", typeof(Int32)));

            return dt;

        }
 
        public int WriteToSqlLog(long line_id, string msg, string line, string datafile)
        {
            SqlConnection conn = new SqlConnection(GetSqlConnStr());
            SqlCommand cmd = new SqlCommand();
            conn.Open();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = @"sp_executesql";// this.GetSelect();
            cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)).Value = 
                @"insert  "+ _LOG_TABLE + @" (line_id,msg,line,datafile,dvid)
                    values(@line_id, @msg, @line, @datafile, @dvid)";
            cmd.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar)).Value = _PARAMS_ERR;
            cmd.Parameters.Add(new SqlParameter("line_id", SqlDbType.Int)).Value = line_id;
            cmd.Parameters.Add(new SqlParameter("msg", SqlDbType.NVarChar)).Value = msg;
            cmd.Parameters.Add(new SqlParameter("line", SqlDbType.NVarChar)).Value = line;
            cmd.Parameters.Add(new SqlParameter("datafile", SqlDbType.NVarChar)).Value = datafile;
            if ((_LocalDS != null) && (_LocalDS.Tables.Count > 0)
                && (_LocalDS.Tables["FileInfo"] != null) && (_LocalDS.Tables["FileInfo"].Rows.Count > 0))
                cmd.Parameters.Add(new SqlParameter("dvid", SqlDbType.BigInt)).Value = long.Parse(_LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString());
            else
                cmd.Parameters.Add(new SqlParameter("dvid", SqlDbType.BigInt)).Value = -9999;

            int rows = cmd.ExecuteNonQuery();
            return rows;
        }
        protected List<string> _WARN_PKs;
        public void WriteAllToWarnLog(List<FormatWarning> warn_list, bool debug)
        {
            if (debug)
            {
                WriteToSqlWarnLog(_SQL_PARAMETERS, new string[7] { "*", "*", "*", "*", "*", "0", DateTime.Now.ToShortDateString() }.ToList());
                WriteToSqlLog((int)999, "split", "****", "*");
            }            
            foreach (FormatWarning fw in warn_list)
            {
                WriteToSqlWarnLog(_SQL_PARAMETERS, fw.GetAllValues());
                if (debug)
                    WriteToSqlLog((int)fw.LineNumber, fw.Message == null ? "" : fw.Message.ToString(), fw.FieldName.Quotename("[") + "=" + fw.FieldValue, "");
            }
        }
 
        public SqlCommand SqlParamsWithValues(List<string> values)
        {
            SqlCommand cmd = new SqlCommand();
            List<SqlParameter> parameters = _SQL_PARAMETERS;
            int pcnt = parameters.Count();
            for (int i = 0; i < values.Count() && i < pcnt; i++)
            {
                if (values.Count() > i)
                {
                    if (parameters[i].ParameterName == "line_num")
                        parameters[i].Value = long.Parse(values[i].ToString());
                    else
                    if (parameters[i].ParameterName == "date")
                        parameters[i].Value = DateTime.Now;
                    else
                        parameters[i].Value = values[i].ToString();
                }
                else
                {
                    if (parameters[i].ParameterName == "line_num")
                        parameters[i].Value = -1;
                    else
                    if (parameters[i].ParameterName == "date")
                        parameters[i].Value = DateTime.Now;
                    else
                        parameters[i].Value = DBNull.Value.ToString();
                }
            }
            cmd.Parameters.AddRange(parameters.Select(p => p.Value).ToArray());
            return cmd;
        }

        public int FinalWriteToSqlWarnLog(List<FormatWarning> warn_list, int batch, int pos)
        { 
            if ((_LocalDS.Tables.Count == 0) || (_LocalDS.Tables["FileInfo"].Rows.Count == 0) || (warn_list.Count == 0))
                return 0;
            int rows = 0;
            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                SqlCommand cmd = new SqlCommand();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar));
                StringBuilder sb = new StringBuilder();

                if (warn_list.Skip(pos).Take(batch).Count() > 0)
                {
                    sb.AppendFormat("insert {0}({1})", _LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString()
                    , string.Join(",\n", _localWARN.Columns.OfType<DataColumn>().Select(c => c.ColumnName.Quotename("[")).ToArray()));
                    sb.AppendLine("values " + string.Join(",\n"
                        , warn_list.Skip(pos).Take(batch).Select(v => string.Format("({0},{1},{2})", string.Join(","
                        , v.GetAllValues().Select(x => !x.StartsWith("'") && !x.EndsWith("'") ? x.Quotename("'") : x))
                        , DateTime.Now.ToString().Quotename("'")
                        , _LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString())).ToList()));
                    cmd.Parameters[0].Value = sb.ToString();
                    rows = cmd.ExecuteNonQuery();              
                } 
                return rows;
            }
        }
        public int FinalWarnWriteToSqlWarnLog(List<FormatWarning> warn_list, int batch, int position)
        {
            //List<string> err_values = Program.warn_list.Tol; // err_values0.Select(c => c).ToList();
            if ((_LocalDS.Tables.Count == 0) || (_LocalDS.Tables["FileInfo"].Rows.Count == 0) || (warn_list.Count == 0))
                return 0;
            int rows = 0;
            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                SqlCommand cmd = new SqlCommand();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar));
                StringBuilder sb = new StringBuilder();
                //batch = batch / 4;
                int loops = (warn_list.Count - position) / (batch == 0 ? 1 : batch);
                loops = ((warn_list.Count - position) % (batch == 0 ? 1 : batch) > 0) ? loops + 1 : loops;
                if ((loops == 1) || (batch > (warn_list.Count - position)))
                    batch = warn_list.Count;
                int pos = position;
                while (loops > 0)
                {
                    if (warn_list.Skip(pos).Take(batch).Count() > 0)
                    {
                        sb.AppendFormat("insert {0}({1})", _LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString()
                        , string.Join(",\n", _localWARN.Columns.OfType<DataColumn>().Select(c => c.ColumnName.Quotename("[")).ToArray()));
                        sb.AppendLine("values " + string.Join(",\n"
                            , warn_list.Skip(pos).Take(batch).Select(v => string.Format("({0},{1},{2})", string.Join(","
                            , v.GetAllValues().Select(x => !x.StartsWith("'") && !x.EndsWith("'") ? x.Quotename("'") : x))
                            , DateTime.Now.ToString().Quotename("'")
                            , _LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString())).ToList()));
                        cmd.Parameters[0].Value = sb.ToString();
                        int r = cmd.ExecuteNonQuery();

                        //if ((r == batch) || (r == err_values.Count))
                        //err_values.RemoveRange(0, batch);
                        loops--;
                        rows += r;
                        pos += r;
                        sb.Clear();
                    }
                }
                //conn.Close();
                return rows;
            }
        }
        public int WriteToWarnLog(List<SqlParameter> pars, List<string> values)
        {
            
            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                //long dvid = long.Parse(_LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString());
                //SqlParameter pdvid = new SqlParameter("dvid", SqlDbType.BigInt);
                //pdvid.Value = dvid;
                SqlCommand cmd = new SqlCommand();
                cmd.Parameters.Clear();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)).Value = _WARN_CMD_STMT;
                cmd.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar)).Value = _WARN_PARAMS;

                //pars.Where(p => p.ParameterName == "date").FirstOrDefault().Value = DateTime.Now.ToShortDateString();
                int vals = values.Count();
                for (int i = 0; i < pars.Count(); i++)
                {
                    if ((vals > i) && (!cmd.Parameters.Contains(pars[i])))
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, long.Parse(values[i].ToString()));
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, DateTime.Now);
                        else
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, values[i].ToString());
                    }
                    else
                    if ((vals <= i) && (!cmd.Parameters.Contains(pars[i])))
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters[pars[i].ParameterName].Value = -1;
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters[pars[i].ParameterName].Value = DateTime.Now;
                        //else
                        //if (pars[i].ParameterName != "dvid")
                        //    cmd.Parameters[pars[i].ParameterName].Value = long.Parse(_LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString());
                        else
                            cmd.Parameters[pars[i].ParameterName].Value = DBNull.Value.ToString();
                    }
                    else
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters[pars[i].ParameterName].Value = -1;
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters[pars[i].ParameterName].Value = DateTime.Now;
                        else
                            cmd.Parameters[pars[i].ParameterName].Value = DBNull.Value.ToString();
                    }
                }

                int rows = cmd.ExecuteNonQuery();

                //conn.Close();
                return rows;
            }
        }

        public int WriteToSqlWarnLog(List<SqlParameter> pars, List<string> values)
        {
            using (SqlConnection conn = new SqlConnection(GetSqlConnStr()))
            {
                SqlCommand cmd = new SqlCommand();
                cmd.Parameters.Clear();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)).Value = _WARN_CMD_STMT;
                cmd.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar)).Value = _WARN_PARAMS;                
                
                //pars.Where(p => p.ParameterName == "date").FirstOrDefault().Value = DateTime.Now.ToShortDateString();
                int vals = values.Count();
                for (int i = 0; i < pars.Count(); i++)
                {
                    if ((vals > i) && (!cmd.Parameters.Contains(pars[i])))
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, long.Parse(values[i].ToString()));
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, DateTime.Now);
                        else
                            cmd.Parameters.AddWithValue(pars[i].ParameterName, values[i].ToString());
                    }
                    else
                    if ((vals <= i) && (!cmd.Parameters.Contains(pars[i])))
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters[pars[i].ParameterName].Value = -1;
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters[pars[i].ParameterName].Value = DateTime.Now;
                        else
                            cmd.Parameters[pars[i].ParameterName].Value = DBNull.Value.ToString();
                    }
                    else
                    {
                        if (pars[i].ParameterName == "line_num")
                            cmd.Parameters[pars[i].ParameterName].Value = -1;
                        else
                        if (pars[i].ParameterName == "date")
                            cmd.Parameters[pars[i].ParameterName].Value = DateTime.Now;
                        else
                            cmd.Parameters[pars[i].ParameterName].Value = DBNull.Value.ToString();
                    }
                }                          

                int rows = cmd.ExecuteNonQuery();

                //conn.Close();
                return rows;
            }
        }


        public void PrintLocalDataSet(DataSet tgt_ds)
        {
            foreach(DataTable dtab in tgt_ds.Tables)
                PrintTableData(dtab, true);
            Console.WriteLine(string.Format("Table: {0, 30}\tPrimary Key: [{1}]"
                , tgt_ds.Tables[0].TableName
                , string.Join(",", tgt_ds.Tables[0].PrimaryKey.ToList())));
            Console.WriteLine(string.Format("Table: {0, 30}\tPrimary Key Constraint: [{1}]"
                , tgt_ds.Tables[0].Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == true).FirstOrDefault().ConstraintName.ToString()
                , string.Join(",", tgt_ds.Tables[0].Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().Columns.OfType<DataColumn>().Select(con => con.ColumnName).ToList())));
 
            Console.WriteLine(string.Format("Table (TGT): {0, 30}\tForeign Key: [{1}]"
                , tgt_ds.Tables[0].Constraints.OfType<ForeignKeyConstraint>().FirstOrDefault().RelatedTable.TableName.ToUpper()
                , string.Join(",", tgt_ds.Tables[0].Constraints.OfType<ForeignKeyConstraint>().FirstOrDefault().Columns.Select(col => col.ColumnName).ToList())));
            Console.WriteLine(string.Format("Table: {0, 30}\tUnique Constraint: [{1}]"
                , tgt_ds.Tables[0].Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().ConstraintName.ToString()
                , string.Join(",", tgt_ds.Tables[0].Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().Columns.OfType<DataColumn>().Select(con => con.ColumnName).ToList())));
        }
        public void PrintDataTableMetadata(DataTable dtab)
        {
            if (dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == true).ToList().Count > 0)
                Console.WriteLine(string.Format("Table: {0, 30}\tPrimary Key Constraint: [{1}]\tCols: {2}"
                , dtab.TableName, dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == true).FirstOrDefault().ConstraintName.ToString()
                , string.Join(",", dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().Columns.OfType<DataColumn>().Select(con => con.ColumnName).ToList())));
            if (dtab.Constraints.OfType<ForeignKeyConstraint>().ToList().Count > 0)
                Console.WriteLine(string.Format("Table: {0, 30}\tForeign Key: [{1}]"
                , dtab.Constraints.OfType<ForeignKeyConstraint>().FirstOrDefault().RelatedTable.TableName.ToUpper()
                , string.Join(",", dtab.Constraints.OfType<ForeignKeyConstraint>().FirstOrDefault().Columns.Select(col => col.ColumnName).ToList())));
            if (dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).ToList().Count > 0)
                Console.WriteLine(string.Format("Table: {0, 30}\tUnique Constraint: [{1}]"
                , dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().ConstraintName.ToString()
                , string.Join(",", dtab.Constraints.OfType<UniqueConstraint>().Where(c => c.IsPrimaryKey == false).FirstOrDefault().Columns.OfType<DataColumn>().Select(con => con.ColumnName).ToList())));
        }
        public void PrintTableData(DataTable dtab, bool header, int maxcols = 10, char divider = '|')
        {
            int len = PrintTableHeaders(dtab);
            int r = maxcols;
            //ItemArray.ToList()));
            foreach (DataRow drow in dtab.Rows)
            {
                Console.WriteLine((header ? divider.ToString() : "") + string.Join(",", drow.ItemArray.ToList())+ (header ? divider.ToString() : ""));
                r -= 1;
                if (r <= 0)
                    break;
            }
            if (header)
            {  
                Console.WriteLine(new string('-', len));
            }
        }
        public void PrintTableSchema(DataTable dtab, bool header)
        {
            int len = PrintTableHeaders(dtab);
            foreach (DataColumn dcol in dtab.Columns)
            {

                Console.WriteLine(string.Format("Type: {0,9}\tColumn: {1,25}\tUNIQUE: {2}\tNULL: {3}\tLEN: {4}\tREADONLY: {5}\tDEF: {6}\tIDENT: {7}"
                    , dcol.DataType.ToString(), dcol.ColumnName, dcol.Unique.ToString()
                    , dcol.AllowDBNull.ToString()
                    , dcol.MaxLength.ToString(), dcol.ReadOnly.ToString(), dcol.DefaultValue.ToString()
                    , dcol.AutoIncrement.ToString()));
            }
            if (header)
            {
                Console.WriteLine(new string('-', len));
            }
        }
        public int PrintTableHeaders(DataTable dtab, char divider = '|')
        {
            string hdr = string.Join(divider.ToString(), dtab.Columns.OfType<DataColumn>().Select(c => c.ColumnName));
            Console.WriteLine(new string('-', hdr.Length));
            Console.WriteLine(hdr);
            Console.WriteLine(new string('-', hdr.Length));
            return hdr.Length;
        }
    }
}


 
