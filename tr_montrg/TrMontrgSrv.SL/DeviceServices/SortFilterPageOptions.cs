using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CS.MI.TrMontrgSrv.EF.QueryObjects;

namespace CS.MI.TrMontrgSrv.EF
{
    public class SortFilterPageOptions
    {
        #region Fields

        private const int DEFAULT_PAGE_SIZE = 10;
        private int _pageNum = 1;
        private int _pageSize = DEFAULT_PAGE_SIZE;

        #endregion

        #region Properties

        public int DefaultPageSize { get; } = DEFAULT_PAGE_SIZE;

        /// <summary>
        /// This holds the possible page sizes
        /// </summary>
        public int[] PageSizes
        {
            get
            {
                return new int[] { 5, DEFAULT_PAGE_SIZE, 20, 50, 100, 1000 };
            }
        }

        public OrderByOptions OrderByOptions { get; set; }

        public LocationFilterBy FilterBy { get; set; }


        public string FilterValue { get; set; }

        public int PageNum
        {
            get { return _pageNum; }
            set { _pageNum = value; }
        }

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = value; }
        }

        /// <summary>
        /// This is set to the number of pages available based on the number of entries in the query
        /// </summary>
        public int NumPages { get; private set; }

        /// <summary>
        /// This holds the state of the key parts of the SortFilterPage parts
        /// </summary>
        public string PrevCheckState { get; set; }

        #endregion

        #region Public Methods

        public void SetupRestOfDto<T>(IQueryable<T> query)
        {
            NumPages = (int)Math.Ceiling((double)query.Count() / PageSize);
            PageNum = Math.Min(Math.Max(1, PageNum), NumPages);

            var newCheckState = GenerateCheckState();
            if (PrevCheckState != newCheckState)
                PageNum = 1;

            PrevCheckState = newCheckState;
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// This returns a string containing the state of the SortFilterPage data
        /// that, if they change, should cause the PageNum to be set back to 0
        /// </summary>
        /// <returns></returns>
        private string GenerateCheckState()
        {
            return $"{(int)FilterBy},{FilterValue},{PageSize},{NumPages}";
        }

        #endregion
    }
}
