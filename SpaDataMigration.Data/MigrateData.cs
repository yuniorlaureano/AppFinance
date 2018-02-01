using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AppsFinance.Data.Repository;
using AppsFinance.Entity;
using System.Data.SqlClient;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace AppsFinance.Data
{
    public enum ENVVAR
    {
        PRI = 21,
        DOM = 45
    }

    public class MigrateData
    {
        private OracleBasicsOperations oraclebasicOperations;
        public Country Country { get; set; }
        public ENVVAR Envvar { get; set; }

        public MigrateData(Country country, ENVVAR envvar)
        {
            this.oraclebasicOperations = new OracleBasicsOperations();
            this.Country = country;
            this.Envvar = envvar;
        }

        #region stract the data from de database
        public List<string> GetStatement(string procedure)
        {
            OracleDataReader reader = null;
            List<string> createdStatements = null;
            OracleParameter[] prm = {
                                        new OracleParameter{ ParameterName = "envvar", OracleDbType = OracleDbType.Int32, Value = (int) this.Envvar},
                                        new OracleParameter{ ParameterName = "resultset", Direction = ParameterDirection.Output, OracleDbType = OracleDbType.RefCursor}                                        
                                    };

            try
            {
                reader = this.oraclebasicOperations.ExecuteDataReader("pkg_appsfinance_" + this.Envvar.ToString() + ".sp_get_" + procedure, prm, CommandType.StoredProcedure, Schema.APPS, this.Country);
                createdStatements = CreateStatement(reader);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                this.oraclebasicOperations.CloseConnection();
            }

            return createdStatements;
        }

        public List<string> GetCviewerCustResumeStatement()
        {
            OracleDataReader reader = null;
            List<string> createdStatements = null;
            OracleParameter[] prm = {
                                        new OracleParameter{ ParameterName = "envvar", OracleDbType = OracleDbType.Int32, Value = (int) this.Envvar},
                                        new OracleParameter { ParameterName = "resultset", Direction = ParameterDirection.Output, OracleDbType = OracleDbType.RefCursor } 
                                    };

            try
            {
                reader = this.oraclebasicOperations.ExecuteDataReader("pkg_appsfinance_" + this.Envvar.ToString() + ".sp_get_invoice_cust_data", prm, CommandType.StoredProcedure, Schema.APPS, this.Country);
                createdStatements = CreateCviewerCustResumeStatement(reader);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                this.oraclebasicOperations.CloseConnection();
            }

            return createdStatements;
        }

        public List<string> GetCviewerCollectorStatement()
        {
            OracleDataReader reader = null;
            List<string> createdStatements = null;
            OracleParameter[] prm = {
                                        new OracleParameter{ ParameterName = "envvar", OracleDbType = OracleDbType.Int32, Value = (int) this.Envvar},
                                        new OracleParameter { ParameterName = "resultset", Direction = ParameterDirection.Output, OracleDbType = OracleDbType.RefCursor } 
                                    };

            try
            {
                reader = this.oraclebasicOperations.ExecuteDataReader("pkg_appsfinance_" + this.Envvar.ToString() + ".sp_get_collector_info", prm, CommandType.StoredProcedure, Schema.APPS, this.Country);
                createdStatements = CreateCviewerCollectorStatement(reader);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                this.oraclebasicOperations.CloseConnection();
            }

            return createdStatements;
        }

        public List<string> GetCviewerPaymentDetailStatement()
        {
            OracleDataReader reader = null;
            List<string> createdStatements = null;
            OracleParameter[] prm = {
                                        new OracleParameter{ ParameterName = "envvar", OracleDbType = OracleDbType.Int32, Value = (int) this.Envvar},
                                        new OracleParameter { ParameterName = "resultset", Direction = ParameterDirection.Output, OracleDbType = OracleDbType.RefCursor } 
                                    };

            try
            {
                reader = this.oraclebasicOperations.ExecuteDataReader("pkg_appsfinance_" + this.Envvar.ToString() + ".sp_get_payment_info", prm, CommandType.StoredProcedure, Schema.APPS, this.Country);
                createdStatements = CreateCviewerPaymentDetailStatement(reader);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                this.oraclebasicOperations.CloseConnection();
            }

            return createdStatements;
        }

        #endregion


        #region Insert the data
        public void InsertStatement(List<string> statements, string procedure, string tranvencetype)
        {

            OracleParameter[] prm = 
            {
                new OracleParameter { ParameterName = "in_statement", OracleDbType = OracleDbType.Clob, Value = "" },
                new OracleParameter { ParameterName = "in_tran_vence_type", OracleDbType = OracleDbType.Varchar2, Value = tranvencetype }
            };
            
            try
            {
                foreach (var st in statements)
                {
                    prm[0].Value = st;
                    this.oraclebasicOperations.ExecuteNonQuery(procedure, prm, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
                }
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }

        public void InsertCviewerCustResumeStatement(List<string> statements)
        {

            OracleParameter[] prm = 
            {
                new OracleParameter { ParameterName = "in_statement", OracleDbType = OracleDbType.Clob, Value = "" }
            };

            try
            {
                foreach (var st in statements)
                {
                    prm[0].Value = st;
                    this.oraclebasicOperations.ExecuteNonQuery("pkg_appsfinance.cViewer_add_cust_resumen", prm, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
                }
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }

        public void InsertCviewerCollectorStatement(List<string> statements)
        {

            OracleParameter[] prm = 
            {
                new OracleParameter { ParameterName = "in_statement", OracleDbType = OracleDbType.Clob, Value = "" }
            };

            try
            {
                foreach (var st in statements)
                {
                    prm[0].Value = st;
                    this.oraclebasicOperations.ExecuteNonQuery("pkg_appsfinance.cViewer_add_collector_detail", prm, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
                }
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }

        public void InsertCviewerPaymentStatement(List<string> statements)
        {

            OracleParameter[] prm = 
            {
                new OracleParameter { ParameterName = "in_statement", OracleDbType = OracleDbType.Clob, Value = "" }
            };

            try
            {
                foreach (var st in statements)
                {
                    prm[0].Value = st;
                    this.oraclebasicOperations.ExecuteNonQuery("pkg_appsfinance.cViewer_add_payment_detail", prm, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
                }
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }
        #endregion

        #region delete data
        public void DeleteDueAging()
        {
            try
            {
                this.oraclebasicOperations.ExecuteNonQuery("pkg_appsfinance.sp_delete_due_inv_aging", null, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }

        public void DeleteCustCollectorPayment()
        {
            try
            {
                this.oraclebasicOperations.ExecuteNonQuery("pkg_appsfinance.sp_del_cust_collectr_paymnt", null, CommandType.StoredProcedure, Schema.YBRDS_PROD, this.Country);
            }
            catch (SqlException except)
            {
                throw except;
            }
            catch (ArgumentNullException except)
            {
                throw except;
            }
            finally
            {
                this.oraclebasicOperations.CloseConnection();
            }
        }
        #endregion

        #region Crea prepara la data a insertar
        public List<string> CreateStatement(OracleDataReader reader)
        {
            List<string> statements = new List<string>();
            string statement = string.Empty;
            int count = 0;
            try
            {
                while (reader.Read())
                {
                    statement += "select " + reader["account_number"] + " AS subscr_id, '" + reader["trx_number"] + "' AS trx_number, '" + reader["trx_date"] + "' AS trx_date, '" + reader["due_date"] + "' AS due_date, " + reader["amount"] + " AS amount, " + reader["remaining"] + " AS remaining from dual union ";

                    count += 1;
                    if (count == 1000)
                    {
                        count = 0;
                        statements.Add(statement.Substring(0, statement.Length - 7));
                        statement = string.Empty;
                    }
                }

                if (count > 0 && count < 1000)
                {
                    statements.Add(statement.Substring(0, statement.Length - 7));
                    statement = string.Empty;
                }


            }
            catch (Exception except)
            {
                throw except;

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                oraclebasicOperations.CloseConnection();
            }

            return statements;
        }

        public List<string> CreateCviewerCustResumeStatement(OracleDataReader reader)
        {
            List<string> statements = new List<string>();
            string statement = string.Empty;
            int count = 0;
            try
            {
                while (reader.Read())
                {
                    statement += "select '" + CleanCustormerName(reader["Customer_Name"].ToString()) + "' AS Customer_Name, " + reader["Customer_Number"] + " AS Customer_Number, '" + CleanCustormerName(reader["Collector_Name"].ToString()) + "' AS Collector_Name, " + reader["Outstanding_Amount"] + " AS Outstanding_Amount, " + reader["Corriente"] + " AS Corriente, " + reader["Due_30"] + " AS Due_30, " + reader["Due_60"] + " AS Due_60, " + reader["Due_90"] + " AS Due_90, " + reader["Due_120"] + " AS Due_120, " + reader["Over_120"] + " AS Over_120, '" + reader["TRAN_TYPE"] + "' AS TRAN_TYPE from dual union ";

                    count += 1;
                    if (count == 1000)
                    {
                        count = 0;
                        statements.Add(statement.Substring(0, statement.Length - 7));//reader["Customer_Name"]
                        statement = string.Empty;
                    }
                }

                if (count > 0 && count < 1000)
                {
                    statements.Add(statement.Substring(0, statement.Length - 7));
                    statement = string.Empty;
                }


            }
            catch (Exception except)
            {
                throw except;

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                oraclebasicOperations.CloseConnection();
            }

            return statements;
        }

        public List<string> CreateCviewerCollectorStatement(OracleDataReader reader)
        {
            List<string> statements = new List<string>();
            string statement = string.Empty;
            int count = 0;
            try
            {
                while (reader.Read())
                {
                    statement += "select " + reader["subscr_id"] + " AS subscr_id, " + reader["account_number_identifier"] + " AS account_number_identifier, '" + CleanCustormerName(reader["name"].ToString()) + "' AS name, '" + reader["call_date"] + "' AS call_date, '" + CleanCustormerName(reader["contact"].ToString()) + "' AS contact from dual union ";

                    count += 1;
                    if (count == 1000)
                    {
                        count = 0;
                        statements.Add(statement.Substring(0, statement.Length - 7));
                        statement = string.Empty;
                    }
                }

                if (count > 0 && count < 1000)
                {
                    statements.Add(statement.Substring(0, statement.Length - 7));
                    statement = string.Empty;
                }


            }
            catch (Exception except)
            {
                throw except;

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                oraclebasicOperations.CloseConnection();
            }

            return statements;
        }

        public List<string> CreateCviewerPaymentDetailStatement(OracleDataReader reader)
        {
            List<string> statements = new List<string>();
            string statement = string.Empty;
            int count = 0;
            try
            {
                while (reader.Read())
                {
                    statement += "select " + reader["account_number"] + " AS account_number, '" + CleanCustormerName(reader["receipt"].ToString()) + "' AS receipt, '" + reader["deposit_date"] + "' AS deposit_date, " + reader["amount"] + " AS amount, '" + reader["status"] + "' AS status, '" + CleanCustormerName(reader["source"].ToString()) + "' AS source from dual union ";

                    count += 1;
                    if (count == 1000)
                    {
                        count = 0;
                        statements.Add(statement.Substring(0, statement.Length - 7));
                        statement = string.Empty;
                    }
                }

                if (count > 0 && count < 1000)
                {
                    statements.Add(statement.Substring(0, statement.Length - 7));
                    statement = string.Empty;
                }


            }
            catch (Exception except)
            {
                throw except;

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                oraclebasicOperations.CloseConnection();
            }

            return statements;
        }
        #endregion
    
        private string CleanCustormerName(string name)
        {
            if (!string.IsNullOrEmpty(name))
            {
                return name.Replace("'","''");
            }

            return name;
        }

    }
}
