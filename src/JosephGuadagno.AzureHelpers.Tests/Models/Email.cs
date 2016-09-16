﻿using System;

namespace JosephGuadagno.AzureHelpers.Tests.Models
{
	/// <summary>
	/// Represents an email that needs to be sent
	/// </summary>
	[Serializable]
	public class Email
	{
		/// <summary>
		/// The recipient of the email
		/// </summary>
		public string ToMailAddress { get; set; }
		/// <summary>
		/// The display name of the recipient
		/// </summary>
		public string ToDisplayName { get; set; }
		/// <summary>
		/// Who the email was from
		/// </summary>
		public string FromMailAddress { get; set; }
		/// <summary>
		/// The display name of the person receiving the email
		/// </summary>
		public string FromDisplayName { get; set; }
		/// <summary>
		/// The subject of the email
		/// </summary>
		public string Subject { get; set; }
		/// <summary>
		/// The body of the email
		/// </summary>
		public string Body { get; set; }
	}
}