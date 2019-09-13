/*
 * DbMapperRecord.cs - Database table row class.
 * (c) 2013  Osvaldo Souza
 * 
*/

using System;
using System.Data.OleDb;
using System.Collections.Generic;
using System.Dynamic;
using Microsoft.CSharp.RuntimeBinder;

namespace Z1.Db
{
    public class DbMapperRecord : Westwind.Utilities.Expando
    {

        public string Prefix = null;
        public string Mapper = null;

        private DbMapper _mapper = null;

        public Dictionary<string, object> dictionary = new Dictionary<string, object>();

        public DbMapperRecord(DbMapper mapper = null)
        {

            if (mapper != null)
            {
                this._mapper = mapper;
            }
            else if (this.Mapper != null)
            {
                this._mapper = (DbMapper)System.Activator.CreateInstance(Type.GetType(this.Mapper));
            }

        }

        public int Count
        {
            get
            {
                return dictionary.Count;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            string name = binder.Name;
            return dictionary.TryGetValue(name, out result);
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            dictionary[binder.Name] = value;
            return true;
        }

        /// <summary>
        /// Find row by primary key
        /// </summary>

        public void Find(dynamic pk)
        {

            if (this.Mapper != null)
            {

                int mode = -1;
                string[,] row;

                if (Db.getFetchMode() != Db.FETCH_ARRAY)
                {
                    mode = Db.getFetchMode();
                    Db.setFetchMode(Db.FETCH_ARRAY);
                }

                this._mapper = (DbMapper)System.Activator.CreateInstance(Type.GetType(this.Mapper));
                this._mapper.Flush();

                row = this._mapper.Where(this._mapper.Primary + " = " + Convert.ToString(pk)).FetchRow();
                this.Hydrate(row);

                if(mode>-1)
                    Db.setFetchMode(mode);

            }

        }

        /// <summary>
        /// Save row
        /// </summary>

        public void Save()
        {
            if (this.Mapper != null)
            {

                this._mapper = (DbMapper)System.Activator.CreateInstance(Type.GetType(this.Mapper));
                this._mapper.Flush();

                this._mapper.Save(this);

            }
        }

        /// <summary>
        /// Delete row
        /// </summary>

        public void Delete()
        {

            if (this.Mapper != null)
            {

                int i = 0;
                string attr = "";

                this._mapper = (DbMapper)System.Activator.CreateInstance(Type.GetType(this.Mapper));
                this._mapper.Flush();

                if (this._mapper.Primary.IndexOf("_") > -1)
                {
                    string[] result = this._mapper.Primary.Split(new string[] { "_" }, StringSplitOptions.None);
                    foreach (string value in result)
                    {
                        attr += (this.Prefix != value) ? char.ToUpper(value[0]) + value.Substring(1).ToLower() : "";
                        i++;
                    }
                }
                else
                {
                    attr = char.ToUpper(this._mapper.Primary[0]) + this._mapper.Primary.Substring(1).ToLower();
                }

                //pk = Convert.ToString(this.GetType().GetProperty(attr).GetValue(this, null));

                this._mapper.Delete(this._mapper.Primary + " = " + this.dictionary[attr]);

            }

        }

        /// <summary>
        /// Hydrate row object
        /// </summary>

        public void Hydrate(string[,] values)
        {

            if (values != null)
            {

                int i, len = values.Length / 2;
                string attr;

                for (i = 0; i < len; i++)
                {
                    attr = "";
                    if (values[i, 0].IndexOf("_") > -1)
                    {
                        string[] result = values[i, 0].Split(new string[] { "_" }, StringSplitOptions.None);
                        foreach (string val in result)
                            attr += (this.Prefix != val) ? char.ToUpper(val[0]) + val.Substring(1).ToLower() : "";
                    }
                    else
                    {
                        attr = char.ToUpper(values[i, 0][0]) + values[i, 0].Substring(1).ToLower();
                    }

                    var binder = Binder.SetMember(CSharpBinderFlags.None,
                                       attr,
                                       typeof(string),
                                       new List<CSharpArgumentInfo>{
                                           CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null),
                                           CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null)
                                       });
                    this.TrySetMember((SetMemberBinder)binder, values[i, 1]);

                }

            }

        }

        /// <summary>
        /// Return row as array
        /// </summary>

        public string[,] ToArray()
        {
            int i = 0;
            string[,] values = new string[this.dictionary.Count, 2];

            foreach(var pair in this.dictionary) {
                values[i, 0] = (this.Prefix != null) ? this.Prefix + "_" + pair.Key.ToUpper() : pair.Key.ToUpper();
                values[i, 1] = Convert.ToString(pair.Value);
                i++;
            }

            return values;

        }

    }
}