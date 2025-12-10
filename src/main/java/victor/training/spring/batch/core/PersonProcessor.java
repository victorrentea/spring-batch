package victor.training.spring.batch.core;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import victor.training.spring.batch.core.domain.City;
import victor.training.spring.batch.core.domain.CityRepo;
import victor.training.spring.batch.core.domain.Person;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
// 1 instance of this class per job run => e ok sa tii state in ea
public class PersonProcessor implements ItemProcessor<PersonXml, Person> {
    @Autowired
    private CityRepo cityRepo;

    private Map<String, City> cityCache;

    @PostConstruct
    void loadAllCitiesFromDb() {
        cityCache = cityRepo.findAll().stream()
                .collect(toMap(City::getName, Function.identity()));
    }

    public Person process(PersonXml xml) {// x N elem / chunk
        Person entity = new Person();
        entity.setName(xml.getName());
//        if (!cityCache.containsKey(xml.getCity())) {
//            var cityOpt = cityRepo.findByName(xml.getCity());
//            if (cityOpt.isPresent()) {
//                cityCache.put(xml.getCity(), cityOpt.get());
//            } else {
//                City city = cityRepo.save(new City(xml.getCity()));
//                cityCache.put(xml.getCity(), city);
//            }
//        }
        entity.setCity(cityCache.get(xml.getCity())); // 100% hit, pentru ca am facut 1st pass pt cities
        return entity;
    }

}
