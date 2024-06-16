import java.net.{ServerSocket, Socket}
import java.io._

object SimpleServer {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(9999)

    val serverAddress = serverSocket.getInetAddress
    println("Server is running on ip " + serverAddress + " port 9999...")

    while (true) {
      val clientSocket = serverSocket.accept()
      println("Client connected: " + clientSocket.getInetAddress.getHostAddress)

      val out = new PrintWriter(clientSocket.getOutputStream, true)
      out.println("hello beijing,hello beijing")
      
      println("Send successfully")
      out.close()
      clientSocket.close()
    }

    serverSocket.close()
  }
}
