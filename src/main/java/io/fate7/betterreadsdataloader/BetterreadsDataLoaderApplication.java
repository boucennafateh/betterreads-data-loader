package io.fate7.betterreadsdataloader;

import io.fate7.betterreadsdataloader.author.Author;
import io.fate7.betterreadsdataloader.author.AuthorRepository;
import io.fate7.betterreadsdataloader.book.Book;
import io.fate7.betterreadsdataloader.book.BookRepository;
import io.fate7.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	@Autowired
	private AuthorRepository authorRepository;

	@Autowired
	private BookRepository bookRepository;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);

	}

	private void initAuthor() {
		Path path = Paths.get(authorDumpLocation);

		try(Stream<String> lines = Files.lines(path)){

			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jsonObject = new JSONObject(jsonString);


					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));

					System.out.println("saving the authaur " + author.getName());
					authorRepository.save(author);

				} catch (JSONException e){
					e.printStackTrace();
				}

			});


		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try(Stream<String> lines = Files.lines(path)){

			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					Book book = new Book();
					book.setName(jsonObject.optString("title"));
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					JSONObject jsonDescription = jsonObject.optJSONObject("description");
					if(jsonDescription != null)
						book.setDescription(jsonDescription.getString("value"));
					JSONObject jsonCreated = jsonObject.optJSONObject("created");
					if(jsonCreated != null){
						String dateStr = jsonCreated.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
					}
					JSONArray jsonCovers = jsonObject.optJSONArray("covers");
					if(jsonCovers != null){
						List<String> coverList = new ArrayList<>();
						for(int i=0; i<jsonCovers.length(); i++)
							coverList.add(jsonCovers.getString(i));
						book.setCoverIds(coverList);
					}

					JSONArray jsonAuthors = jsonObject.optJSONArray("authors");
					if(jsonAuthors != null){
						List<String> authorIdList = new ArrayList<>();
						List<String> authorNameList = new ArrayList<>();

						for(int i=0; i<jsonAuthors.length(); i++){
							JSONObject jsonAuthor = jsonAuthors.getJSONObject(i).optJSONObject("author");
							if(jsonAuthor != null) {
								String key = jsonAuthor.getString("key").replace("/authors/", "");
								authorIdList.add(key);
								Optional<Author> byId = authorRepository.findById(key);
								if(byId.isPresent())
									authorNameList.add(byId.get().getName());
								else
									authorNameList.add("unknown author");


							}

						}
						book.setAuthorIds(authorIdList);
						book.setAuthorNames(authorNameList);

						System.out.println("saving the book " + book.getName());
						bookRepository.save(book);
					}

				} catch (JSONException e){
					e.printStackTrace();
				}

			});


		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void loadData(){

		initAuthor();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
