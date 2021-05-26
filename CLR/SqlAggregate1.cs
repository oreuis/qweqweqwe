using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using Microsoft.SqlServer.Server;

namespace SqlAggregate1.SqlServerStatistical.Aggregates
{
    [Serializable]
    [SqlUserDefinedAggregate(
       Format.UserDefined, 
      IsInvariantToDuplicates = false, 
      IsInvariantToNulls = false, 
      IsInvariantToOrder = true, 
      IsNullIfEmpty = true, 
      Name = "Median",
      MaxByteSize = -1 
    )]
    public struct MedianAggregate : IBinarySerialize
    {
        private List<SqlDouble> _accumulatedItems;

        public List<SqlDouble> AccumulatedItems
        {
            get { return _accumulatedItems; }
        }

        public void Init()
        {
            _accumulatedItems = new List<SqlDouble>();
        }

        public void Accumulate(SqlDouble value)
        {
            if (!value.IsNull)
            {
                _accumulatedItems.Add(value);
            }
        }

        public void Merge(MedianAggregate group)
        {
            _accumulatedItems.AddRange(group.AccumulatedItems);
        }

        public SqlDouble Terminate()
        {
            return CalculateMedian();
        }

        private SqlDouble CalculateMedian()
        {
            SqlDouble medianValue;
            _accumulatedItems.Sort();
            if (_accumulatedItems.Count % 2 == 1)
            {
                int medianIndex = (_accumulatedItems.Count + 1) / 2 - 1;
                medianValue = _accumulatedItems[medianIndex];
            }
            else
            {
                int medianIndexElem1 = _accumulatedItems.Count / 2;
                int medianIndexElem2 = _accumulatedItems.Count / 2 - 1;
                medianValue = (_accumulatedItems[medianIndexElem1] + 
                    _accumulatedItems[medianIndexElem2]) / 2.0;
            }

            return medianValue;
        }


        /// <summary>
        /// Format
        /// Bytes 1 - 4: list items count
        /// Byter 5 - 5+8xlist.Count : 8 byte floating point numbers
        /// </summary>
        /// <param name="reader"></param>
        public void Read(BinaryReader reader)
        {
            _accumulatedItems = new List<SqlDouble>();
            int itemsCount = reader.ReadInt32();
            for (int i = 0; i < itemsCount; i++)
            {
                _accumulatedItems.Add(
                    new SqlDouble(reader.ReadDouble()));
            }
        }

        public void Write(BinaryWriter writer)
        {
            writer.Write(_accumulatedItems.Count);
            foreach (var value in _accumulatedItems)
            {
                writer.Write(value.Value);
            }
        }
    }
}