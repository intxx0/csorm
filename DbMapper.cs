/*
 * DbMapper.cs - Database table mapper class.
 * (c) 2013  Osvaldo Souza
 * 
*/

using System;
using System.Data.OleDb;
using System.Collections.Generic;

namespace Z1.Db
{
    public class DbMapper
    {
        public string Name;
        public string Record;
        public string Primary;

        public dynamic[] _from;
        public string[] _fields;
        public string[][] _where;
        public string[][] _joinInner;
        public string[][] _joinLeft;
        public string[] _order;
        public string[] _group;
        public string _having = null;
        public string _limit = null;
        public int[] _offset;

        public Dictionary<string, Dictionary<string, string>> _metaInfo;

        private Db _database = null;

        public DbMapper()
        {

            this.Flush();

            if (this._database == null)
            {
                this._database = Db.GetInstance();
                this._database.connect();
            }

        }

        public void Flush()
        {

            this._from = new dynamic[0];
            this._fields = new string[0];
            this._where = new string[0][];
            this._joinInner = new string[0][];
            this._joinLeft = new string[0][];
            this._order = new string[0];
            this._group = new string[0];
            this._offset = new int[0];

        }

        /// <summary>
        /// From tables clause
        /// </summary>

        public DbMapper From(string[] tables)
        {
            int len = this._from.Length;
            Array.Resize<dynamic>(ref this._from, (len + tables.Length));
            Array.Copy(tables, 0, this._from, len, tables.Length);
            return this;
        }

        /// <summary>
        /// From tables clause
        /// </summary>

        public DbMapper From(string table)
        {
            int len = this._from.Length;
            Array.Resize<dynamic>(ref this._from, (len + 1));
            this._from[len] = table;
            return this;
        }

        /// <summary>
        /// Columns selection
        /// </summary>

        public DbMapper Fields(string[] fields)
        {
            int len = this._fields.Length;
            Array.Resize<string>(ref this._fields, (len + fields.Length));
            Array.Copy(fields, 0, this._fields, len, fields.Length);
            return this;
        }

        /// <summary>
        /// Single column selection
        /// </summary>

        public DbMapper Field(string field)
        {
            int len = this._fields.Length;
            Array.Resize<string>(ref this._fields, (len + 1));
            this._fields[len] = field;
            return this;
        }

        /// <summary>
        /// Where clause
        /// </summary>

        public DbMapper Where(string expr, string oper = "AND")
        {

            int len = this._where.Length;
            string[] param = new string[2] { expr, oper };

            Array.Resize<string[]>(ref this._where, len + 1);

            this._where[len] = param;

            return this;

        }

        /// <summary>
        /// Join inner table clause
        /// </summary>

        public DbMapper JoinInner(string table, string where)
        {
            int len = this._joinInner.Length;
            string[] param = new string[2] { table, where };

            Array.Resize<string[]>(ref this._joinInner, len + 1);

            this._joinInner[len] = param;

            return this;
        }

        /// <summary>
        /// Join left table clause
        /// </summary>

        public DbMapper JoinLeft(string table, string where)
        {
            int len = this._joinLeft.Length;
            string[] param = new string[2] { table, where };

            Array.Resize<string[]>(ref this._joinLeft, len + 1);

            this._joinLeft[len] = param;

            return this;
        }

        /// <summary>
        /// Result order clause
        /// </summary>

        public DbMapper Order(string order)
        {
            int len = this._order.Length;
            Array.Resize<string>(ref this._order, (len + 1));
            this._order[len] = order;
            return this;
        }

        /// <summary>
        /// Single column grouping clause
        /// </summary>

        public DbMapper Group(string group)
        {
            int len = this._group.Length;
            Array.Resize<string>(ref this._group, (len + 1));
            this._group[len] = group;
            return this;
        }

        /// <summary>
        /// Columns grouping clause
        /// </summary>

        public DbMapper Group(string[] group)
        {
            int len = this._group.Length;
            Array.Resize<string>(ref this._group, (len + group.Length));
            Array.Copy(group, 0, this._group, len, group.Length);
            return this;
        }

        /// <summary>
        /// Having clause
        /// </summary>

        public DbMapper Having(string expr)
        {
            if (this._group.Length>0)
                this._having = expr;

            return this;

        }

        /// <summary>
        /// Result limit
        /// </summary>

        public DbMapper Top(string limit)
        {
            this._limit = limit;
            return this;
        }

        /// <summary>
        /// Select result offset rows
        /// </summary>

        public DbMapper Offset(int start, int end)
        {
            int len = this._offset.Length;
            Array.Resize<int>(ref this._offset, (len + 2));
            this._offset[0] = start;
            this._offset[1] = end;
            return this;
        }

        /// <summary>
        /// Fetch all rows
        /// </summary>

        public dynamic FetchAll()
        {
            return this._database.fetchAll(this);
        }

        /// <summary>
        /// Fetch single row
        /// </summary>

        public dynamic FetchRow()
        {
            return this._database.fetchRow(this);
        }

        /// <summary>
        /// Insert row
        /// </summary>

        public void Insert(string[,] values)
        {
            this._database.insert(this, values);
        }

        /// <summary>
        /// Update row
        /// </summary>

        public void Update(string[,] values, string where)
        {
            this._database.update(this, values, where);
        }

        /// <summary>
        /// Delete row
        /// </summary>

        public void Delete(string where)
        {
            this._database.delete(this, where);
        }

        /// <summary>
        /// Save row
        /// </summary>

        public void Save(dynamic record)
        {
            int i = 0;
            string attr = "";

            if (this.Primary.IndexOf("_") > -1)
            {
                string[] result = this.Primary.Split(new string[] { "_" }, StringSplitOptions.None);
                foreach (string value in result)
                {
                    attr += (record.Prefix != value) ? char.ToUpper(value[0]) + value.Substring(1).ToLower() : "";
                    i++;
                }
            }
            else
            {
                attr = char.ToUpper(this.Primary[0]) + this.Primary.Substring(1).ToLower();
            }

            //pk = Convert.ToString(record.GetType().GetProperty(attr).GetValue(record, null));

            if (record.dictionary.ContainsKey(attr) == false)
            {
                this._database.insert(this, record.ToArray());
            } else {
                this._database.update(this, record.ToArray(), this.Primary + " = " + record.dictionary[attr]);
            }

        }


        /// <summary>
        /// Return query as string
        /// </summary>

        public override string ToString()
        {
            return this._database._buildSelect(this);
        }


    }
}