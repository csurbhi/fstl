#include<iostream>
#include<fstream>
#include<unordered_map>
#include<string>
#include<algorithm>
#include<vector>
#include<sstream>


int main()
{
	std::unordered_map<unsigned long, unsigned long> lba_map;

	std::ifstream lbaFile("lbaLenCompile.txt");
	if (!lbaFile.is_open()) {
		std::cerr <<"Error opening the file!" << std::endl;
		return -1;
	}
	std::string line;
	unsigned long lba, len, count;
	while(std::getline(lbaFile, line)) {
		// std::cout << line << std::endl;
		std::istringstream iss{line};
		if(iss >> lba >> count) {
			std::cout << "LBA: " << lba;
			std::cout << "len: " << len;
		}
		for(int i=0; i<len; i++) {
			auto search = lba_map.find(lba);
			if (search != lba_map.end()) {
				count = search->second;
			} else {
				count = 0;
			}
			count = count + 1;
			lba_map.insert({lba, count});
		}
	}
	lbaFile.close();
	// Now will sort the pairs based on the count
	std::vector<std::pair<unsigned long, unsigned long>> vec_of_pairs;
	for(const auto &pair : lba_map) {
		vec_of_pairs.push_back(pair);
	}

	std::sort(vec_of_pairs.begin(), vec_of_pairs.end(), 
		[] (const std::pair<unsigned long, unsigned long>&a, const std::pair<unsigned long, unsigned long> &b) {
			return a.second > b.second; // Descending order
		}
	);
	for(const auto & pair: vec_of_pairs) {
		std::cout<< pair.first << "=> " << pair.second << std::endl;
	}
	std::cout << "\n";
	return 0;
}
