using System;

namespace CSG.MI.DTO.Feedback
{
	public class Feedback
	{
		public long? Seq { get; set; } = 0;

		public string System { get; set; } = String.Empty;

        public string Category{ get; set; } = String.Empty; // Mster Table - value

        public string Comment { get; set; } = String.Empty;

        public DateTime EditDate { get; set; } = DateTime.Now;
	}
}
