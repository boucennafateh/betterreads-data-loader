package io.fate7.betterreadsdataloader;

import io.fate7.betterreadsdataloader.author.Author;
import io.fate7.betterreadsdataloader.author.AuthorRepository;
import io.fate7.betterreadsdataloader.connection.DataStaxAstraProperties;
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

	@PostConstruct
	public void loadData(){

		initAuthor();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
