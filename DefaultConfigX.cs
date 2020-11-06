using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csvtosql
{
    class DefaultConfig
    {




        internal sealed partial class startup : global::System.Configuration.ApplicationSettingsBase
        {

            private static startup defaultInstance = ((startup)(global::System.Configuration.ApplicationSettingsBase.Synchronized(new startup())));

            public static startup Default
            {
                get
                {
                    return defaultInstance;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("(localdb)\\mssqllocaldb")]
            public string SERVER
            {
                get
                {
                    return ((string)(this["SERVER"]));
                }
                set
                {
                    this["SERVER"] = value;
                }
            }

 
            [global::System.Configuration.DefaultSettingValueAttribute("TESTBED")]
            public string DATABASE
            {
                get
                {
                    return ((string)(this["DATABASE"]));
                }
                set
                {
                    this["DATABASE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("DATABASE")]
            public string METASOURCE
            {
                get
                {
                    return ((string)(this["METASOURCE"]));
                }
                set
                {
                    this["METASOURCE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("DATABASE")]
            public string LOG
            {
                get
                {
                    return ((string)(this["LOG"]));
                }
                set
                {
                    this["LOG"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("[dv].[lkup_files]")]
            public string METATABLE
            {
                get
                {
                    return ((string)(this["METATABLE"]));
                }
                set
                {
                    this["METATABLE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("./")]
            public string PATH
            {
                get
                {
                    return ((string)(this["PATH"]));
                }
                set
                {
                    this["PATH"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("\\*.\\*")]
            public string FILE_RX
            {
                get
                {
                    return ((string)(this["FILE_RX"]));
                }
                set
                {
                    this["FILE_RX"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("\"")]
            public char QUOTE
            {
                get
                {
                    return ((char)(this["QUOTE"]));
                }
                set
                {
                    this["QUOTE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute(",")]
            public char DELIM
            {
                get
                {
                    return ((char)(this["DELIM"]));
                }
                set
                {
                    this["DELIM"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("False")]
            public bool DEBUG
            {
                get
                {
                    return ((bool)(this["DEBUG"]));
                }
                set
                {
                    this["DEBUG"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("5000")]
            public int BATCH
            {
                get
                {
                    return ((int)(this["BATCH"]));
                }
                set
                {
                    this["BATCH"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("2")]
            public int OUTPUT
            {
                get
                {
                    return ((int)(this["OUTPUT"]));
                }
                set
                {
                    this["OUTPUT"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("@\"_out_rows_\" + DateTime.Now.ToString(\"yyyy-dd-M--HH-mm-ss\") + @\".csv\"")]
            public string OUTPUT_ROWS_FILE
            {
                get
                {
                    return ((string)(this["OUTPUT_ROWS_FILE"]));
                }
                set
                {
                    this["OUTPUT_ROWS_FILE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute(" \"_err_\" + DateTime.Now.ToString(\"yyyy-dd-M--HH-mm-ss\")+ @\".csv\"")]
            public string ERR_LOG_FILE
            {
                get
                {
                    return ((string)(this["ERR_LOG_FILE"]));
                }
                set
                {
                    this["ERR_LOG_FILE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("")]
            public string OUTPUT_ROWS_TABLE
            {
                get
                {
                    return ((string)(this["OUTPUT_ROWS_TABLE"]));
                }
                set
                {
                    this["OUTPUT_ROWS_TABLE"] = value;
                }
            }

            [global::System.Configuration.DefaultSettingValueAttribute("")]
            public string ERR_LOG_TABLE
            {
                get
                {
                    return ((string)(this["ERR_LOG_TABLE"]));
                }
                set
                {
                    this["ERR_LOG_TABLE"] = value;
                }
            }
        }
    }
}
