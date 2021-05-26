using System;
using System.Data;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Collections.Generic;

/*Aggregate function*/
[Serializable]
[SqlUserDefinedAggregate(
    Format.UserDefined,               /*impt because we are using Queue<StringJoinModel> 
                                       _values, if StringBuilder been used, 
                                       we could use Format.Native*/
    IsInvariantToDuplicates = false,
    IsInvariantToNulls = true,
    IsInvariantToOrder = true,
    IsNullIfEmpty = true,
    MaxByteSize = 8000,               /*impt: because we used 
                                       Format.UserDefined, 8000 is max*/
    Name = "STRING_JOIN_AGG")]
public class StringJoinAgg : IBinarySerialize   /*impt: IBinarySerialize because 
                                                 we used Format.UserDefined*/
{
    public const string DefaultSeparator = ",";

    class StringJoinModel
    {
        public string Value { get; set; }
        public string Separator { get; set; }
    }

    private Queue<StringJoinModel> _values;

    /// <summary>  
    /// Initialize the internal data structures  
    /// </summary>  
    public void Init()
    {
        _values = new Queue<StringJoinModel>();
    }

    /// <summary>  
    /// Accumulate the next value, not if the value is null  
    /// </summary>  
    /// <param name="value">value to be aggregated</param>  
    /// <param name="separator">separator to be used for concatenation</param>  
    public void Accumulate(SqlString value, SqlString separator)
    {
        if (value.IsNull || String.IsNullOrEmpty(value.Value))
        {
            /*not include null or empty value */
            return;
        }
        string valueString = value.Value;
        string separatorString = separator.IsNull ||
           String.IsNullOrEmpty(separator.Value) ? DefaultSeparator : separator.Value;
        _values.Enqueue(new StringJoinModel
        { Value = valueString, Separator = separatorString });
    }

    /// <summary>  
    /// Merge the partially computed aggregate with this aggregate  
    /// </summary>  
    /// <param name="group">The other partial results to be merged</param>  
    public void Merge(StringJoinAgg group)
    {
        while (group._values.Count != 0)
        {
            _values.Enqueue(group._values.Dequeue());
        }
    }

    /// <summary>  
    /// Called at the end of aggregation, to return the results of the aggregation.  
    /// </summary>  
    /// <returns>Concatenates the elements, 
    /// using the specified separator between each element or member.</returns>  
    public SqlString Terminate()
    {
        StringBuilder builder = new StringBuilder();

        StringJoinModel model;
        if (_values.Count != 0)
        {
            /*first time no separator*/
            model = _values.Dequeue();
            builder.Append(model.Value);
        }
        while (_values.Count != 0)
        {
            model = _values.Dequeue();
            builder.Append(model.Separator).Append(model.Value);
        }

        string value = builder.ToString(); return new SqlString(value);
    }

    /*IBinarySerialize: How read, write should actually work
     * https://stackoverflow.com/questions/27781904/
     * what-are-ibinaryserialize-interface-methods-used-for
     */
    public void Read(BinaryReader r)
    {
        if (r == null)
        {
            throw new ArgumentNullException("r");
        }

        /*
         * Read as write worked
         * --------------------
         * total
         * value1
         * separator1
         * value2
         * separator3  
         * 
         * 
         * valueN
         * separatorN
         */
        _values = new Queue<StringJoinModel>();
        var count = r.ReadInt32();
        for (int i = 0; i < count; i++)
        {
            var model = new StringJoinModel
            {
                Value = r.ReadString(),
                Separator = r.ReadString()
            };
            _values.Enqueue(model);
        }
    }

    public void Write(BinaryWriter w)
    {
        if (w == null)
        {
            throw new ArgumentNullException("w");
        }

        /*
         * Write sample
         * ------------
         * total
         * value1
         * separator1
         * value2
         * separator3  
         * 
         * 
         * valueN
         * separatorN
         */
        w.Write(_values.Count);
        while (_values.Count != 0)
        {
            StringJoinModel m = _values.Dequeue();
            w.Write(m.Value);
            w.Write(m.Separator);
        }
    }
}

