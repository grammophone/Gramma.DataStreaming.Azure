using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace Grammophone.DataStreaming.Azure
{
	/// <summary>
	/// Streamer for reading and writing to an Azure blob container.
	/// </summary>
	public class AzureBlobStreamer : Streamer
	{
		#region Private fields

		private Lazy<CloudStorageAccount> lazyAccount;

		#endregion

		#region Construction

		/// <summary>
		/// Create.
		/// </summary>
		public AzureBlobStreamer()
		{
			lazyAccount = new Lazy<CloudStorageAccount>(ConnectToStorage, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);
		}

		#endregion

		#region Public properties

		/// <summary>
		/// Connection string to Azure blob storage account.
		/// </summary>
		public string ConnectionString { get; set; }

		/// <summary>
		/// The container's name where the files reside.
		/// </summary>
		public string ContainerName { get; set; } = "Container";

		#endregion

		#region Public methods

		/// <summary>
		/// Open a stream for reading.
		/// </summary>
		/// <param name="filename">The filename of the blob inside the container.</param>
		/// <returns>Returns a stream for reading.</returns>
		/// <exception cref="FileNotFoundException">Thrown when the file was not found.</exception>
		/// <exception cref="IOException">Thrown when there is a general reading error.</exception>
		public override Stream OpenReadStream(string filename)
		{
			if (filename == null) throw new ArgumentNullException(nameof(filename));

			var blob = GetBlobReference(filename);

			return blob.OpenRead();
		}

		/// <summary>
		/// Open a stream for writing.
		/// </summary>
		/// <param name="filename">The filename of the blob inside the container.</param>
		/// <param name="overwrite">If true and the file exists, overwrite, else throw an <see cref="IOException"/>.</param>
		/// <returns>Returns a stream for writing.</returns>
		/// <exception cref="IOException">Thrown when there file already exists or there is a general writing error.</exception>
		public override Stream OpenWriteStream(string filename, bool overwrite = true)
		{
			if (filename == null) throw new ArgumentNullException(nameof(filename));

			var blob = GetBlobReference(filename);

			if (!overwrite && blob.Exists())
				throw new IOException($"The file '{filename}' already exists in Azure blob container '{this.ContainerName}'.");

			return blob.OpenWrite();
		}

		#endregion

		#region Private methods

		private CloudStorageAccount ConnectToStorage()
		{
			return CloudStorageAccount.Parse(this.ConnectionString);
		}

		private Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob GetBlobReference(string filename)
		{
			var client = lazyAccount.Value.CreateCloudBlobClient();

			var container = client.GetContainerReference(this.ContainerName);

			var blob = container.GetBlockBlobReference(filename);

			return blob;
		}

		#endregion
	}
}
